"""
Rescheduler

Implements query migration between services based on waiting time analysis.
"""

import time
import numpy as np
from typing import Dict, List, Tuple, Optional
from ..core import Query, ServiceType
from .scheduler import Scheduler
from ..cost_model import CostModel


class Rescheduler:
    """
    Rescheduler

    Responsibilities:
    - Monitor query waiting times in each service queue
    - Identify queries eligible for migration (Formula 16)
    - Estimate waiting times based on resource usage (Formula 19)
    - Select optimal migration target (Formula 20)
    - Prevent ping-pong migrations
    """

    def __init__(self, cost_model: CostModel, scheduler: Scheduler,
                 service_configs: Dict[ServiceType, Dict], config: Dict = None):
        """
        Initialize rescheduler

        Args:
            cost_model: Cost model for estimating time and cost
            scheduler: Scheduler to access queues
            service_configs: Configuration for each service type
            config: Configuration parameters
        """
        self.cost_model = cost_model
        self.scheduler = scheduler
        self.service_configs = service_configs
        self.config = config or {}

        # Migration parameters (Formula 16)
        self.gamma = self.config.get('gamma', 1.0)  # γ: wait time multiplier
        self.tau = self.config.get('tau', 5.0)       # τ: minimum delay bound

        # Cost weight for migration decision (Formula 20)
        self.alpha = self.config.get('alpha', 1.0)   # α: cost weight

        # Track query submission times and migration history
        self.query_submit_times: Dict[str, float] = {}
        self.query_migration_count: Dict[str, int] = {}
        self.last_migration_time: Dict[str, float] = {}

        # Migration control
        self.max_migrations = self.config.get('max_migrations', 3)
        self.migration_cooldown = self.config.get('migration_cooldown', 10.0)

    async def check_and_migrate(self):
        """
        Check queues and migrate queries if needed

        Implements:
        - Formula 16: Migratable query identification
        - Formula 19: Waiting time estimation
        - Formula 20: Migration target selection
        """
        migrations = []

        # Iterate through all service queues
        for service_type, queue in self.scheduler.queues.items():
            if not queue:
                continue

            # Estimate waiting time for this service (Formula 19)
            waiting_time = self._estimate_waiting_time(service_type)

            # Check each query in the queue
            for query in queue:
                # Track submission time
                if query.query_id not in self.query_submit_times:
                    self.query_submit_times[query.query_id] = time.time()

                # Calculate actual waiting time
                current_wait = time.time() - self.query_submit_times[query.query_id]

                # Check if query is eligible for migration (Formula 16)
                if self._is_migratable(query, service_type, current_wait):
                    # Find best migration target (Formula 20)
                    target_service = self._select_migration_target(
                        query, service_type, waiting_time
                    )

                    if target_service and target_service != service_type:
                        migrations.append((query, service_type, target_service))

        # Execute migrations
        for query, source_service, target_service in migrations:
            self._migrate_query(query, source_service, target_service)

    def _is_migratable(self, query: Query, service_type: ServiceType,
                      current_wait: float) -> bool:
        """
        Check if query is eligible for migration (Formula 16)

        W_q^(s) > max(γ · T̂_q,s, τ)

        Args:
            query: Query to check
            service_type: Current service type
            current_wait: Current waiting time

        Returns:
            True if query is eligible for migration
        """
        # Check migration count limit
        migration_count = self.query_migration_count.get(query.query_id, 0)
        if migration_count >= self.max_migrations:
            return False

        # Check migration cooldown
        if query.query_id in self.last_migration_time:
            time_since_last = time.time() - self.last_migration_time[query.query_id]
            if time_since_last < self.migration_cooldown:
                return False

        # Get estimated execution time for this query on current service
        if not query.resource_requirements:
            return False

        estimated_time = query.estimated_time if query.estimated_time else 0.0

        # Apply Formula 16: W_q^(s) > max(γ · T̂_q,s, τ)
        threshold = max(self.gamma * estimated_time, self.tau)

        return current_wait > threshold

    def _estimate_waiting_time(self, service_type: ServiceType) -> float:
        """
        Estimate waiting time for a service (Formula 19)

        Ŵ^(s) = max_{i=1,...,k} (Σ_{q∈Q^(s)} Σ_{j=1}^t R̂_q^(s)[i,j] + Σ_{j=1}^t Ĉ_used^(s)[i,j]) / c_capacity^(s)[i] · Δt

        Args:
            service_type: Service type to estimate

        Returns:
            Estimated waiting time in seconds
        """
        # Get all executor instances for this service
        executor_ids = [
            eid for eid in self.scheduler.executor_states.keys()
            if eid.startswith(service_type.value)
        ]

        if not executor_ids:
            return 0.0

        # Use the first executor's state (or average across all)
        # For simplicity, we use the first one
        executor_id = executor_ids[0]
        state = self.scheduler.executor_states[executor_id]

        k, t = state.capacity_matrix_shape

        # Calculate total resource demand from queued queries
        queue = self.scheduler.queues.get(service_type, [])
        total_demand = np.zeros((k, t))

        for query in queue:
            if query.query_id in self.scheduler.query_demands:
                demand = self.scheduler.query_demands[query.query_id]
                total_demand += demand.matrix

        # Add resource usage from running queries
        capacity_matrix = state.remaining_capacity.reshape((k, t))
        initial_capacity = state._initialize_capacity(k, t).reshape((k, t))
        used_capacity = initial_capacity - capacity_matrix

        # Calculate waiting time for each resource dimension
        waiting_times = []
        for i in range(k):
            resource_load = np.sum(total_demand[i, :]) + np.sum(used_capacity[i, :])
            capacity = initial_capacity[i, 0]  # Capacity per time slot

            if capacity > 0:
                # Number of time slots needed
                slots_needed = resource_load / capacity
                # Convert to seconds (assuming 1 second per slot)
                wait_time = slots_needed * 1.0
                waiting_times.append(wait_time)

        # Return maximum waiting time across all resource dimensions
        return max(waiting_times) if waiting_times else 0.0

    def _select_migration_target(self, query: Query, current_service: ServiceType,
                                 current_wait: float) -> Optional[ServiceType]:
        """
        Select optimal migration target service (Formula 20)

        s* = argmin_{s∈S} (Ŵ^(s) + T̂_q,s) · M̂_q,s^α

        Args:
            query: Query to migrate
            current_service: Current service type
            current_wait: Current waiting time

        Returns:
            Target service type, or None if no better option
        """
        if not query.resource_requirements:
            return None

        from ..core import ResourceRequirements
        resources = ResourceRequirements(
            cpu_time=query.resource_requirements.get('cpu_time', 0),
            data_scanned=query.resource_requirements.get('data_scanned', 0),
            scale_factor=query.resource_requirements.get('scale_factor', 50.0)
        )

        best_service = None
        best_score = float('inf')

        # Evaluate all available services
        for service_type in [ServiceType.VM, ServiceType.FAAS, ServiceType.QAAS]:
            # Estimate waiting time for this service (Formula 19)
            waiting_time = self._estimate_waiting_time(service_type)

            # Get service configuration
            service_config = self.service_configs.get(service_type, {})

            # Estimate execution time and cost using router model
            if service_type == ServiceType.VM:
                estimate = self.cost_model.estimate_vm(resources, service_config)
            elif service_type == ServiceType.FAAS:
                estimate = self.cost_model.estimate_faas(resources, service_config)
            elif service_type == ServiceType.QAAS:
                estimate = self.cost_model.estimate_qaas(resources, service_config)
            else:
                continue

            # Calculate score using Formula 20: (Ŵ^(s) + T̂_q,s) · M̂_q,s^α
            score = (waiting_time + estimate.execution_time) * (estimate.cost ** self.alpha)

            if score < best_score:
                best_score = score
                best_service = service_type

        # Calculate current service score for comparison
        current_estimate_time = query.estimated_time if query.estimated_time else 0.0
        current_estimate_cost = query.estimated_cost if query.estimated_cost else 0.0
        current_score = (current_wait + current_estimate_time) * (current_estimate_cost ** self.alpha)

        # Only migrate if new service is significantly better
        if best_service and best_score < current_score * 0.9:  # 10% improvement threshold
            return best_service

        return None

    def _migrate_query(self, query: Query, source_service: ServiceType,
                      target_service: ServiceType):
        """
        Execute query migration from source to target service

        Args:
            query: Query to migrate
            source_service: Source service type
            target_service: Target service type
        """
        # Remove from source queue
        source_queue = self.scheduler.queues.get(source_service, [])
        if query in source_queue:
            source_queue.remove(query)

        # Remove demand from source
        if query.query_id in self.scheduler.query_demands:
            del self.scheduler.query_demands[query.query_id]

        # Update query routing decision
        query.routing_decision = target_service.value

        # Re-estimate time and cost for target service
        if query.resource_requirements:
            from ..core import ResourceRequirements
            resources = ResourceRequirements(
                cpu_time=query.resource_requirements.get('cpu_time', 0),
                data_scanned=query.resource_requirements.get('data_scanned', 0),
                scale_factor=query.resource_requirements.get('scale_factor', 50.0)
            )

            service_config = self.service_configs.get(target_service, {})

            if target_service == ServiceType.VM:
                estimate = self.cost_model.estimate_vm(resources, service_config)
            elif target_service == ServiceType.FAAS:
                estimate = self.cost_model.estimate_faas(resources, service_config)
            elif target_service == ServiceType.QAAS:
                estimate = self.cost_model.estimate_qaas(resources, service_config)

            query.estimated_time = estimate.execution_time
            query.estimated_cost = estimate.cost

        # Enqueue to target service
        self.scheduler.enqueue(query, target_service)

        # Update migration tracking
        self.query_migration_count[query.query_id] = \
            self.query_migration_count.get(query.query_id, 0) + 1
        self.last_migration_time[query.query_id] = time.time()

        print(f"[Rescheduler] Migrated query {query.query_id}: {source_service.value} to {target_service.value} "
              f"(migrations: {self.query_migration_count[query.query_id]})")

