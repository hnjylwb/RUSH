"""
FaaS (Function as a Service) Executor

Example: AWS Lambda, Azure Functions, GCP Cloud Functions

Executes queries in parallel across multiple Lambda instances with data partitioning.
"""

import time
import asyncio
from typing import Dict, List, Any, Optional
from .base import BaseExecutor
from ..core import Query, ExecutionResult, ExecutionStatus, ServiceConfig


class FaaSExecutor(BaseExecutor):
    """
    FaaS Executor

    Characteristics:
    - Serverless, auto-scaling
    - Billing by execution time × memory
    - Cluster of instances
    """

    def __init__(self, config: ServiceConfig):
        super().__init__(config)

        # FaaS-specific configuration
        self.num_instances = config.config.get('num_instances', 100)
        self.memory_per_instance_gb = config.config.get('memory_per_instance_gb', 4)
        self.cpu_per_instance = config.config.get('cpu_per_instance', 1.2)
        self.io_per_instance_mbps = config.config.get('io_per_instance_mbps', 75)
        self.cost_per_gb_second = config.config.get('cost_per_gb_second', 0.0000002)

        # Lambda function endpoint
        self.endpoint = config.config.get('endpoint')
        if not self.endpoint:
            raise ValueError(f"FaaS executor '{self.name}' missing 'endpoint' configuration")

        if self.endpoint.startswith('<'):
            raise ValueError(
                f"FaaS executor '{self.name}' has placeholder endpoint '{self.endpoint}'. "
                f"Please configure valid endpoint (e.g., 'https://lambda.us-east-1.amazonaws.com/function')"
            )

        # Cluster total capacity
        self.total_cpu = self.num_instances * self.cpu_per_instance
        self.total_io = self.num_instances * self.io_per_instance_mbps

    def _allocate_partitions(self, num_partitions: int, num_instances: int) -> List[List[int]]:
        """
        Allocate partitions to instances evenly

        Args:
            num_partitions: Total number of data partitions
            num_instances: Number of Lambda instances to use

        Returns:
            List of partition lists, one per instance
        """
        # Use minimum of available instances and partitions
        instances_to_use = min(num_instances, num_partitions)

        # Calculate base partitions per instance and remainder
        base_partitions = num_partitions // instances_to_use
        remainder = num_partitions % instances_to_use

        allocation = []
        partition_id = 0

        for i in range(instances_to_use):
            # First 'remainder' instances get one extra partition
            partitions_for_this = base_partitions + (1 if i < remainder else 0)
            instance_partitions = list(range(partition_id, partition_id + partitions_for_this))
            allocation.append(instance_partitions)
            partition_id += partitions_for_this

        return allocation

    async def _invoke_lambda(self, query: Query, partition_ids: List[int],
                            instance_id: int) -> Dict[str, Any]:
        """
        Invoke a single Lambda instance

        Args:
            query: Query to execute
            partition_ids: List of partition IDs for this instance
            instance_id: Instance identifier

        Returns:
            Result from Lambda execution
        """
        try:
            import aiohttp
        except ImportError:
            return {
                'status': 'error',
                'error': 'aiohttp not installed',
                'partition_ids': partition_ids,
                'instance_id': instance_id
            }

        payload = {
            'query_id': query.query_id,
            'sql': query.sql,
            'partition_ids': partition_ids,
            'instance_id': instance_id,
            'metadata': query.metadata
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.endpoint,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        error_text = await response.text()
                        return {
                            'status': 'error',
                            'error': f"HTTP {response.status}: {error_text}",
                            'partition_ids': partition_ids,
                            'instance_id': instance_id
                        }

        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'partition_ids': partition_ids,
                'instance_id': instance_id
            }

    def _aggregate_results(self, lambda_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate results from multiple Lambda instances

        Args:
            lambda_results: List of results from each Lambda

        Returns:
            Aggregated result
        """
        total_rows = 0
        total_execution_time = 0.0
        failed_instances = []
        successful_instances = 0

        for result in lambda_results:
            if result.get('status') == 'success':
                total_rows += result.get('row_count', 0)
                exec_time = result.get('execution_time', 0.0)
                # Max execution time (parallel execution)
                total_execution_time = max(total_execution_time, exec_time)
                successful_instances += 1
            else:
                failed_instances.append({
                    'instance_id': result.get('instance_id'),
                    'partition_ids': result.get('partition_ids'),
                    'error': result.get('error')
                })

        if failed_instances:
            return {
                'status': 'partial' if successful_instances > 0 else 'error',
                'execution_time': total_execution_time,
                'row_count': total_rows,
                'successful_instances': successful_instances,
                'failed_instances': failed_instances
            }
        else:
            return {
                'status': 'success',
                'execution_time': total_execution_time,
                'row_count': total_rows,
                'successful_instances': successful_instances
            }

    def _decide_num_instances(self, query: Query, num_partitions: int) -> int:
        """
        Decide how many Lambda instances to use for this query

        This uses a cost model to determine optimal instance count.
        TODO: Implement actual cost model in next step.

        Args:
            query: Query to execute
            num_partitions: Number of data partitions

        Returns:
            Number of instances to use
        """
        # Simple strategy for now: use all available instances up to partition count
        # TODO: Replace with cost-based model that considers:
        #   - Query complexity
        #   - Data size
        #   - Cost vs performance tradeoff
        #   - Resource requirements
        return min(self.num_instances, num_partitions)

    async def execute(self, query: Query) -> ExecutionResult:
        """
        Execute query on FaaS cluster with data partitioning

        Args:
            query: Query to execute

        Returns:
            ExecutionResult with aggregated results
        """
        start_time = time.time()

        try:
            # Get partition information from query metadata
            num_partitions = query.metadata.get('num_partitions', 100) if query.metadata else 100

            # Determine how many instances to use based on cost model
            instances_to_use = self._decide_num_instances(query, num_partitions)

            # Allocate partitions to instances
            partition_allocation = self._allocate_partitions(num_partitions, instances_to_use)

            print(f"[{self.name}] Query {query.query_id}: Using {instances_to_use} instances "
                  f"for {num_partitions} partitions")

            # Invoke all Lambda instances in parallel
            tasks = []
            for instance_id, partition_ids in enumerate(partition_allocation):
                task = self._invoke_lambda(query, partition_ids, instance_id)
                tasks.append(task)

            # Wait for all instances to complete
            lambda_results = await asyncio.gather(*tasks)

            # Aggregate results
            aggregated = self._aggregate_results(lambda_results)

            # Calculate total time and cost
            total_time = time.time() - start_time
            execution_time = aggregated['execution_time']

            # Calculate cost: instances × execution_time × memory × rate
            cost = (instances_to_use * execution_time * self.memory_per_instance_gb *
                   self.cost_per_gb_second)

            # Determine status
            if aggregated['status'] == 'success':
                status = ExecutionStatus.SUCCESS
                error = None
            elif aggregated['status'] == 'partial':
                status = ExecutionStatus.FAILED
                error = f"{len(aggregated['failed_instances'])} instances failed"
            else:
                status = ExecutionStatus.FAILED
                error = "All instances failed"

            print(f"[{self.name}] Query {query.query_id} completed: "
                  f"{execution_time:.3f}s execution, {total_time:.3f}s total, "
                  f"{aggregated['row_count']} rows, ${cost:.6f}")

            return ExecutionResult(
                query_id=query.query_id,
                service_used=self.name,
                status=status,
                execution_time=execution_time,
                cost=cost,
                error=error,
                metadata={
                    'total_time': total_time,
                    'row_count': aggregated['row_count'],
                    'instances_used': instances_to_use,
                    'successful_instances': aggregated['successful_instances'],
                    'num_partitions': num_partitions,
                    'failed_instances': aggregated.get('failed_instances', [])
                }
            )

        except Exception as e:
            execution_time = time.time() - start_time
            print(f"[{self.name}] Unexpected error: {e}")

            return ExecutionResult(
                query_id=query.query_id,
                service_used=self.name,
                status=ExecutionStatus.FAILED,
                execution_time=execution_time,
                cost=0.0,
                error=f"Unexpected error: {str(e)}"
            )

    def estimate_cost(self, query: Query, estimated_time: float) -> float:
        """
        FaaS cost model: gb_seconds × rate

        Args:
            query: Query object
            estimated_time: Estimated execution time (seconds)

        Returns:
            Estimated cost in dollars
        """
        gb_seconds = estimated_time * self.memory_per_instance_gb * self.num_instances
        return gb_seconds * self.cost_per_gb_second

    def get_capacity(self) -> Dict[str, float]:
        """Get FaaS cluster capacity"""
        return {
            'num_instances': self.num_instances,
            'total_cpu': self.total_cpu,
            'total_io': self.total_io,
            'memory_per_instance_gb': self.memory_per_instance_gb
        }
