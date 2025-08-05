import asyncio
from typing import List, Dict
from ..models.query import Query, ServiceType
from ..executors.base_executor import BaseServiceExecutor
from .query_queue import QueryQueue
from ..config.parameters import RUSHParameters, calculate_migration_threshold

class InterScheduler:
    def __init__(self, services: Dict[ServiceType, List[BaseServiceExecutor]], queues: Dict[ServiceType, QueryQueue]):
        self.services = services
        self.queues = queues
        self.params = RUSHParameters()
        self.running = False
        
    async def start_background_scheduling(self):
        """Start background inter-service scheduling"""
        self.running = True
        while self.running:
            await self._rebalance_queues()
            await asyncio.sleep(self.params.INTER_SCHEDULER_INTERVAL)
    
    def stop_background_scheduling(self):
        """Stop background scheduling"""
        self.running = False
        
    async def _rebalance_queues(self):
        """Rebalance queries between service queues based on wait time thresholds"""
        # Find queries that exceed migration threshold
        candidate_queries = self._find_migration_candidates()
        
        if candidate_queries:
            for query, source_service_type in candidate_queries:
                # Find best alternative service for this query
                target_service_type = self._find_best_alternative_service(query, source_service_type)
                
                if target_service_type and target_service_type != source_service_type:
                    # Migrate query
                    source_queue = self.queues[source_service_type]
                    target_queue = self.queues[target_service_type]
                    
                    if query in source_queue.queue:
                        source_queue.queue.remove(query)
                        target_queue.add_query(query)
                        
                        # Record migration in query history
                        query.record_migration(source_service_type.value, target_service_type.value)
                        
                        print(f"Migrated query {query.id} from {source_service_type.value} to {target_service_type.value} "
                              f"(migration #{query.migration_count})")
    
    def _find_migration_candidates(self) -> List[tuple]:
        """Find queries that exceed migration threshold with cooldown and migration limit"""
        candidates = []
        
        for service_type, queue in self.queues.items():
            for query in queue.queue:
                # Skip if query is in cooldown period
                if query.time_since_last_migration() < self.params.MIGRATION_COOLDOWN_PERIOD:
                    continue
                
                # Skip if query has exceeded maximum migrations
                if query.migration_count >= self.params.MAX_MIGRATIONS_PER_QUERY:
                    continue
                
                # Get predicted latency for this query on current service
                predicted_latency = query.get_predicted_latency(service_type)
                
                # Calculate migration threshold
                threshold = calculate_migration_threshold(predicted_latency)
                
                # Check if query has been waiting longer than threshold
                wait_time = query.get_wait_time()
                
                if wait_time > threshold:
                    candidates.append((query, service_type))
        
        return candidates
    
    def _find_best_alternative_service(self, query: Query, current_service: ServiceType) -> ServiceType:
        """Find best alternative service for migrating query with ping-pong prevention"""
        available_services = [s for s in ServiceType if s != current_service]
        
        best_service = None
        best_score = float('-inf')
        
        for service_type in available_services:
            if service_type in self.services:
                # Simple ping-pong prevention: don't migrate back to a service recently migrated from
                if query.has_recent_migration_to_service(
                    service_type.value, 
                    self.params.PING_PONG_PREVENTION_WINDOW
                ):
                    continue
                
                score = self._calculate_service_score(service_type, query)
                if score > best_score:
                    best_score = score
                    best_service = service_type
        
        return best_service
    
    def _calculate_service_score(self, service_type: ServiceType, query: Query) -> float:
        """Calculate service score: time * cost^a (lower is better)"""
        services = self.services.get(service_type, [])
        if not services:
            return float('inf')  # Worst score if service unavailable
        
        # Get predicted execution time and cost for this query on this service
        execution_time = query.get_predicted_latency(service_type)
        execution_cost = query.get_predicted_cost(service_type)
        
        # Calculate total time = already_waited + predicted_wait + execution_time
        already_waited = query.get_wait_time()
        predicted_wait = self._estimate_queue_wait_time(service_type)
        total_time = already_waited + predicted_wait + execution_time
        
        # Calculate score using cost-performance trade-off parameter
        # Score = time * cost^a (lower is better)
        score = total_time * (execution_cost ** self.params.COST_PERFORMANCE_TRADEOFF)
        
        return score
    
    def _estimate_queue_wait_time(self, service_type: ServiceType) -> float:
        """Estimate wait time based on resource usage of queued queries"""
        queue = self.queues.get(service_type)
        if not queue or queue.get_queue_size() == 0:
            return 0.0  # No wait if queue is empty
        
        # Calculate total resource area (usage * time) for all queued queries
        if service_type == ServiceType.EC2:
            # For EC2, calculate areas for each resource dimension separately
            total_cpu_area = 0.0
            total_memory_area = 0.0
            total_io_area = 0.0
            
            for queued_query in queue.queue:
                if queued_query.resource_time_slots:
                    service_key = service_type.value
                    if service_key in queued_query.resource_time_slots:
                        resource_slots = queued_query.resource_time_slots[service_key]
                        
                        cpu_slots = resource_slots.get('cpu_slots', [])
                        memory_slots = resource_slots.get('memory_slots', [])
                        io_slots = resource_slots.get('io_slots', [])
                        
                        # Calculate area for each resource type
                        cpu_area = sum(usage * self.params.TIME_SLOT_DURATION for usage in cpu_slots)
                        memory_area = sum(usage * self.params.TIME_SLOT_DURATION for usage in memory_slots)
                        io_area = sum(usage * self.params.TIME_SLOT_DURATION for usage in io_slots)
                        
                        total_cpu_area += cpu_area
                        total_memory_area += memory_area
                        total_io_area += io_area
            
            # Get service capacities for each dimension
            service_capacity = self._get_service_capacity(service_type)
            cpu_capacity, memory_capacity, io_capacity = service_capacity
            
            # Calculate wait time for each dimension
            cpu_wait = total_cpu_area / cpu_capacity if cpu_capacity > 0 else 0.0
            memory_wait = total_memory_area / memory_capacity if memory_capacity > 0 else 0.0
            io_wait = total_io_area / io_capacity if io_capacity > 0 else 0.0
            
            # Take maximum wait time (bottleneck resource determines overall wait)
            estimated_wait = max(cpu_wait, memory_wait, io_wait)
        
        else:
            # For Lambda and Athena, use single dimension calculation
            total_resource_area = 0.0
            
            for queued_query in queue.queue:
                if queued_query.resource_time_slots:
                    service_key = service_type.value
                    if service_key in queued_query.resource_time_slots:
                        resource_slots = queued_query.resource_time_slots[service_key]
                        
                        if service_type == ServiceType.LAMBDA:
                            # For Lambda, use instance slots
                            instance_slots = resource_slots.get('instance_slots', [])
                            query_area = sum(usage * self.params.TIME_SLOT_DURATION for usage in instance_slots)
                        
                        elif service_type == ServiceType.ATHENA:
                            # For Athena, use query slots (typically all 1.0)
                            query_slots = resource_slots.get('query_slots', [])
                            query_area = sum(usage * self.params.TIME_SLOT_DURATION for usage in query_slots)
                        
                        else:
                            query_area = 0.0
                        
                        total_resource_area += query_area
            
            # Get service capacity and calculate wait time
            service_capacity = self._get_service_capacity(service_type)
            if service_capacity > 0:
                estimated_wait = total_resource_area / service_capacity
            else:
                estimated_wait = 0.0
        
        return estimated_wait
    
    def _get_service_capacity(self, service_type: ServiceType):
        """Get service capacity in resource units per second"""
        capacities = self.params.SERVICE_CAPACITIES
        
        if service_type == ServiceType.EC2:
            # For EC2, return tuple of (cpu, memory, io) capacities
            ec2_caps = capacities['EC2']
            return (ec2_caps['cpu'], ec2_caps['memory'], ec2_caps['io'])
        elif service_type == ServiceType.LAMBDA:
            return capacities['LAMBDA']
        elif service_type == ServiceType.ATHENA:
            return capacities['ATHENA']
        else:
            raise ValueError(f"Unknown service type: {service_type}")