"""
Scheduler

Resource-aware scheduling within each service type.
Implements Algorithm 1 from the paper.
"""

import asyncio
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from ..core import Query, ServiceType, ExecutionResult
from ..executors import BaseExecutor


@dataclass
class ResourceDemand:
    """
    Resource demand matrix for a query

    Represents resource usage over time slots.
    Shape: (k_resources, t_slots)
    """
    matrix: np.ndarray  # Shape: (k, t)

    def flatten(self) -> np.ndarray:
        """Flatten to vector for compatibility scoring"""
        return self.matrix.flatten()

    def norm(self) -> float:
        """L2 norm of flattened demand"""
        return np.linalg.norm(self.flatten())


@dataclass
class ExecutorState:
    """
    State of a single executor instance

    Tracks remaining capacity and running queries.
    """
    executor: BaseExecutor
    executor_id: str  # Unique ID (e.g., "vm-1-worker-0", "faas-cluster-1")
    remaining_capacity: np.ndarray  # Shape: (k, t) - flattened to (k*t,)
    running_queries: List[Query]
    capacity_matrix_shape: Tuple[int, int]  # (k, t)

    def __init__(self, executor: BaseExecutor, executor_id: str,
                 num_resources: int, num_time_slots: int):
        self.executor = executor
        self.executor_id = executor_id
        self.running_queries = []
        self.capacity_matrix_shape = (num_resources, num_time_slots)

        # Initialize remaining capacity based on executor type
        self.remaining_capacity = self._initialize_capacity(num_resources, num_time_slots)

    def _initialize_capacity(self, k: int, t: int) -> np.ndarray:
        """
        Initialize capacity matrix based on executor configuration

        For VM: capacity is fixed based on VM specs
        For FaaS: capacity is cluster total
        For QaaS: capacity is based on concurrent query limit
        """
        capacity_matrix = np.zeros((k, t))

        # Get executor capacity
        capacity = self.executor.get_capacity()

        # For simplicity, we use a few key resources:
        # Resource 0: CPU cores (or equivalent)
        # Resource 1: Memory (GB)
        # Resource 2: I/O bandwidth (MB/s)

        if hasattr(self.executor, 'cpu_cores'):
            # VM executor
            capacity_matrix[0, :] = capacity.get('cpu_cores', 1)
            capacity_matrix[1, :] = capacity.get('memory_gb', 1)
            capacity_matrix[2, :] = capacity.get('io_bandwidth_mbps', 100)
        elif hasattr(self.executor, 'num_instances'):
            # FaaS executor
            capacity_matrix[0, :] = capacity.get('total_cpu', 1)
            capacity_matrix[1, :] = capacity.get('memory_per_instance_gb', 1) * capacity.get('num_instances', 1)
            capacity_matrix[2, :] = capacity.get('total_io', 100)
        else:
            # QaaS executor
            capacity_matrix[0, :] = capacity.get('max_concurrent_queries', 10)
            capacity_matrix[1, :] = 1000  # Large memory capacity (managed)
            capacity_matrix[2, :] = capacity.get('scan_speed_tb_per_sec', 1) * 1000  # Convert to MB/s

        return capacity_matrix.flatten()

    def can_fit(self, demand: ResourceDemand) -> bool:
        """Check if query demand can fit in remaining capacity"""
        demand_flat = demand.flatten()
        return np.all(demand_flat <= self.remaining_capacity)

    def compute_score(self, demand: ResourceDemand) -> float:
        """
        Compute compatibility score for a query

        Formula from paper: score(q) = (c_remain · r_q) / ||r_q||^2
        """
        demand_flat = demand.flatten()
        demand_norm_sq = np.sum(demand_flat ** 2)

        if demand_norm_sq == 0:
            return 0.0

        # Dot product: alignment with remaining capacity
        alignment = np.dot(self.remaining_capacity, demand_flat)

        score = alignment / demand_norm_sq
        return score

    def allocate(self, demand: ResourceDemand):
        """Allocate resources for a query"""
        self.remaining_capacity -= demand.flatten()

    def release(self, demand: ResourceDemand):
        """Release resources after query completes"""
        self.remaining_capacity += demand.flatten()


class Scheduler:
    """
    Service Scheduler

    Implements resource-aware scheduling within each service type.
    Uses Algorithm 1 from the paper for scheduling decisions.
    """

    def __init__(self, executors: Dict[ServiceType, List[BaseExecutor]],
                 num_resources: int = 3, num_time_slots: int = 10):
        """
        Initialize scheduler

        Args:
            executors: Executors for each service type
            num_resources: Number of resource dimensions (default: 3 for CPU, memory, I/O)
            num_time_slots: Number of time slots in scheduling horizon
        """
        self.num_resources = num_resources
        self.num_time_slots = num_time_slots

        # Initialize executor states - one state per executor instance
        self.executor_states: Dict[str, ExecutorState] = {}

        for service_type, executor_list in executors.items():
            for idx, executor in enumerate(executor_list):
                executor_id = f"{service_type.value}-{idx}"
                state = ExecutorState(
                    executor=executor,
                    executor_id=executor_id,
                    num_resources=num_resources,
                    num_time_slots=num_time_slots
                )
                self.executor_states[executor_id] = state

        # Query queues per service type (not per executor instance)
        self.queues: Dict[ServiceType, List[Query]] = {
            service_type: [] for service_type in executors.keys()
        }

        # Store resource demands for queries
        self.query_demands: Dict[str, ResourceDemand] = {}

    def enqueue(self, query: Query, service_type: ServiceType):
        """
        Add query to service queue

        Args:
            query: Query to enqueue
            service_type: Target service
        """
        self.queues[service_type].append(query)

        # Compute and store resource demand matrix for this query
        self.query_demands[query.query_id] = self._compute_resource_demand(query, service_type)

    def _compute_resource_demand(self, query: Query, service_type: ServiceType) -> ResourceDemand:
        """
        Compute resource demand matrix for a query

        Converts query's resource requirements into a (k, t) matrix.
        For simplicity, we assume uniform distribution over time slots.

        Args:
            query: Query object
            service_type: Service type

        Returns:
            ResourceDemand with matrix of shape (k, t)
        """
        k = self.num_resources
        t = self.num_time_slots

        demand_matrix = np.zeros((k, t))

        if not query.resource_requirements:
            return ResourceDemand(matrix=demand_matrix)

        # Extract resource requirements
        cpu_time = query.resource_requirements.get('cpu_time', 0)
        data_scanned = query.resource_requirements.get('data_scanned', 0)
        scale_factor = query.resource_requirements.get('scale_factor', 50)

        # Estimate resource usage per time slot
        # Resource 0: CPU cores needed
        cpu_per_slot = cpu_time / t
        demand_matrix[0, :] = cpu_per_slot

        # Resource 1: Memory (based on data scanned and scale factor)
        memory_gb = (data_scanned / (1024**3)) * (scale_factor / 100)
        demand_matrix[1, :] = memory_gb

        # Resource 2: I/O bandwidth (MB/s)
        data_mb = data_scanned / (1024**2)
        io_per_slot = data_mb / t
        demand_matrix[2, :] = io_per_slot

        return ResourceDemand(matrix=demand_matrix)

    def get_queue_size(self, service_type: ServiceType) -> int:
        """Get current queue size for a service"""
        return len(self.queues.get(service_type, []))

    def get_queue_sizes_per_executor(self) -> Dict[str, int]:
        """
        Get queue sizes per executor instance

        For VM auto-scaling: each VM instance has its own queue size
        (number of running queries on that instance)
        """
        return {
            executor_id: len(state.running_queries)
            for executor_id, state in self.executor_states.items()
        }

    async def schedule_service(self, service_type: ServiceType) -> List[Tuple[Query, str]]:
        """
        Schedule queries for a specific service type

        Implements Algorithm 1 from the paper.

        Args:
            service_type: Service type to schedule

        Returns:
            List of (query, executor_id) tuples to execute
        """
        scheduled = []

        # Get all executor instances for this service type
        executor_ids = [eid for eid in self.executor_states.keys()
                       if eid.startswith(service_type.value)]

        if not executor_ids:
            return scheduled

        # Get queued queries for this service
        queue = self.queues.get(service_type, [])

        if not queue:
            return scheduled

        # For each executor instance, run scheduling algorithm
        for executor_id in executor_ids:
            state = self.executor_states[executor_id]
            scheduled_for_executor = []

            # Algorithm 1: Resource-Aware Intra-Service Scheduling
            # Line 1: S ← ∅
            # (scheduled_for_executor serves as S)

            # Line 2: F ← {q ∈ Q | r_q <= c_remain}
            feasible = []
            for query in queue:
                if query.query_id in self.query_demands:
                    demand = self.query_demands[query.query_id]
                    if state.can_fit(demand):
                        feasible.append(query)

            # Line 3-9: while F ≠ ∅
            while feasible:
                # Line 4-5: foreach q ∈ F, compute score(q)
                scores = {}
                for query in feasible:
                    demand = self.query_demands[query.query_id]
                    scores[query.query_id] = state.compute_score(demand)

                # Line 6: q* ← argmax score(q)
                best_query_id = max(scores, key=scores.get)
                best_query = next(q for q in feasible if q.query_id == best_query_id)
                best_demand = self.query_demands[best_query_id]

                # Line 7: S ← S ∪ {q*}
                scheduled_for_executor.append(best_query)
                scheduled.append((best_query, executor_id))

                # Line 8: c_remain ← c_remain - r_q*
                state.allocate(best_demand)
                state.running_queries.append(best_query)

                # Remove from queue
                queue.remove(best_query)

                # Line 9: F ← {q ∈ F \ {q*} | r_q <= c_remain}
                feasible = []
                for query in queue:
                    if query.query_id in self.query_demands:
                        demand = self.query_demands[query.query_id]
                        if state.can_fit(demand):
                            feasible.append(query)

        # Line 10: return S
        return scheduled

    async def schedule_and_execute(self, service_type: ServiceType) -> List[ExecutionResult]:
        """
        Schedule and execute queries for a specific service type

        This is the main entry point that:
        1. Runs the scheduling algorithm (Algorithm 1)
        2. Submits scheduled queries to executors
        3. Collects execution results
        4. Releases resources immediately after each query completes

        Args:
            service_type: Service type to schedule and execute

        Returns:
            List of ExecutionResult from completed queries
        """
        # Step 1: Run scheduling algorithm
        scheduled = await self.schedule_service(service_type)

        if not scheduled:
            return []

        # Step 2: Submit queries to executors and execute
        # Each query will release resources immediately upon completion
        execution_tasks = []
        for query, executor_id in scheduled:
            state = self.executor_states[executor_id]
            # Submit query and wrap with resource release
            task = self._execute_and_release(query, executor_id, state)
            execution_tasks.append(task)

        # Step 3: Wait for all queries to complete
        results = await asyncio.gather(*execution_tasks)

        return results

    async def _execute_query(self, query: Query, state: ExecutorState) -> ExecutionResult:
        """
        Execute a single query on an executor

        Args:
            query: Query to execute
            state: Executor state

        Returns:
            ExecutionResult
        """
        try:
            # Execute query on the executor
            result = await state.executor.execute(query)
            return result
        except Exception as e:
            # Handle execution errors
            from ..core import ExecutionStatus
            return ExecutionResult(
                query_id=query.query_id,
                service_used=state.executor_id,
                status=ExecutionStatus.FAILED,
                execution_time=0.0,
                cost=0.0,
                error=f"Execution failed: {str(e)}"
            )

    async def _execute_and_release(self, query: Query, executor_id: str,
                                   state: ExecutorState) -> ExecutionResult:
        """
        Execute a query and immediately release resources upon completion

        Args:
            query: Query to execute
            executor_id: Executor ID
            state: Executor state

        Returns:
            ExecutionResult
        """
        try:
            # Execute query
            result = await self._execute_query(query, state)
            return result
        finally:
            # Always release resources, even if execution failed
            self.release_query(query, executor_id)

    async def schedule_and_execute_all(self) -> Dict[ServiceType, List[ExecutionResult]]:
        """
        Schedule and execute queries for all service types

        Returns:
            Dictionary mapping service types to execution results
        """
        results = {}

        for service_type in self.queues.keys():
            service_results = await self.schedule_and_execute(service_type)
            if service_results:
                results[service_type] = service_results

        return results

    def release_query(self, query: Query, executor_id: str):
        """
        Release resources after query completes

        Args:
            query: Completed query
            executor_id: Executor that ran the query
        """
        if executor_id not in self.executor_states:
            return

        state = self.executor_states[executor_id]

        # Release resources
        if query.query_id in self.query_demands:
            demand = self.query_demands[query.query_id]
            state.release(demand)

        # Remove from running queries
        if query in state.running_queries:
            state.running_queries.remove(query)

        # Clean up demand matrix
        if query.query_id in self.query_demands:
            del self.query_demands[query.query_id]
