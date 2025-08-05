"""
Service-Specific Resource-Aware Intra-Scheduler

This module implements a simplified intra-scheduling algorithm with service-specific resource modeling:
- EC2: CPU/Memory/IO resource modeling
- Lambda: Function instance count modeling  
- Athena: Query concurrency modeling

The core scheduling logic is centralized in IntraScheduler, with service-specific resource profiling.
"""

import asyncio
import time
import numpy as np
from typing import List, Dict, Tuple, Union
from dataclasses import dataclass
from ..models.query import Query, ServiceType
from ..executors.base_executor import BaseServiceExecutor
from .query_queue import QueryQueue
from ..config.parameters import RUSHParameters


@dataclass
class ResourceProfile:
    """Universal resource profile for different service types"""
    service_type: ServiceType
    resource_usage: Union[Tuple[float, float, float], int]  # EC2: (cpu, mem, io), Lambda/Athena: int
    duration: float


class ResourceScheduler:
    """Unified resource scheduler with per-executor resource tracking"""
    
    def __init__(self, service_type: ServiceType, executors: List[BaseServiceExecutor], time_slot_duration: float = 0.5):
        self.service_type = service_type
        self.time_slot_duration = time_slot_duration
        self.executors = executors
        
        # Per-executor resource timelines
        self.executor_timelines: Dict[str, List[Union[Tuple[float, float, float], int]]] = {}
        self.scheduled_queries: List[Tuple[str, str, float, float]] = []  # (query_id, executor_name, start_time, duration)
        
        # Initialize timeline for each executor
        for executor in executors:
            self.executor_timelines[executor.name] = []
        
        # Service-specific capacity limits per executor (from global parameters)
        params = RUSHParameters()
        capacities = params.SERVICE_CAPACITIES
        
        if service_type == ServiceType.EC2:
            ec2_caps = capacities['EC2']
            self.max_capacity = (ec2_caps['cpu'], ec2_caps['memory'], ec2_caps['io'])
        elif service_type == ServiceType.LAMBDA:
            self.max_capacity = capacities['LAMBDA']
        elif service_type == ServiceType.ATHENA:
            self.max_capacity = capacities['ATHENA']
    
    def can_schedule_query_on_executor(self, query: Query, executor_name: str, start_time: float = 0.0) -> bool:
        """Check if query can be scheduled on specific executor using precomputed resource slots"""
        if executor_name not in self.executor_timelines:
            return False
            
        if not query.resource_time_slots:
            print(f"Warning: No resource_time_slots found for query {query.id}")
            return False
        
        # Get service-specific resource slots
        service_key = self.service_type.value
        if service_key not in query.resource_time_slots:
            print(f"Warning: No resource slots for service {service_key}")
            return False
            
        service_slots = query.resource_time_slots[service_key]
        start_slot = int(start_time / self.time_slot_duration)
        timeline = self.executor_timelines[executor_name]
        
        if self.service_type == ServiceType.EC2:
            # EC2: Check CPU, Memory, IO slots
            cpu_slots = service_slots.get('cpu_slots', [])
            memory_slots = service_slots.get('memory_slots', [])
            io_slots = service_slots.get('io_slots', [])
            
            max_slots = max(len(cpu_slots), len(memory_slots), len(io_slots))
            
            # Extend timeline if needed
            needed_slots = start_slot + max_slots
            while len(timeline) < needed_slots:
                timeline.append((0.0, 0.0, 0.0))
            
            # Check each time slot
            for i in range(max_slots):
                slot_idx = start_slot + i
                if slot_idx < len(timeline):
                    current_cpu, current_mem, current_io = timeline[slot_idx]
                    
                    req_cpu = cpu_slots[i] if i < len(cpu_slots) else 0.0
                    req_mem = memory_slots[i] if i < len(memory_slots) else 0.0
                    req_io = io_slots[i] if i < len(io_slots) else 0.0
                    
                    if (current_cpu + req_cpu > self.max_capacity[0] or
                        current_mem + req_mem > self.max_capacity[1] or
                        current_io + req_io > self.max_capacity[2]):
                        return False
        
        else:
            # Lambda/Athena: Use single resource slot array
            if self.service_type == ServiceType.LAMBDA:
                resource_slots = service_slots.get('instance_slots', [])
            else:  # ATHENA
                resource_slots = service_slots.get('query_slots', [])
            
            # Extend timeline if needed
            needed_slots = start_slot + len(resource_slots)
            while len(timeline) < needed_slots:
                timeline.append(0)
            
            # Check against shared pool capacity
            for i, resource_usage in enumerate(resource_slots):
                slot_idx = start_slot + i
                if slot_idx < len(timeline):
                    # Check total usage across all executors
                    total_usage = sum(self.executor_timelines[name][slot_idx] 
                                    for name in self.executor_timelines.keys() 
                                    if slot_idx < len(self.executor_timelines[name]))
                    if total_usage + resource_usage > self.max_capacity:
                        return False
        
        return True
    
    def schedule_query_on_executor(self, query: Query, executor_name: str, start_time: float = 0.0) -> bool:
        """Schedule query on specific executor using precomputed resource slots"""
        # if not self.can_schedule_query_on_executor(query, executor_name, start_time):
        #     return False
        
        if not query.resource_time_slots:
            return False
            
        # Get service-specific resource slots
        service_key = self.service_type.value
        if service_key not in query.resource_time_slots:
            return False
            
        service_slots = query.resource_time_slots[service_key]
        start_slot = int(start_time / self.time_slot_duration)
        timeline = self.executor_timelines[executor_name]
        
        if self.service_type == ServiceType.EC2:
            # EC2: Update CPU, Memory, IO slots
            cpu_slots = service_slots.get('cpu_slots', [])
            memory_slots = service_slots.get('memory_slots', [])
            io_slots = service_slots.get('io_slots', [])
            
            max_slots = max(len(cpu_slots), len(memory_slots), len(io_slots))
            
            # Update resource timeline
            for i in range(max_slots):
                slot_idx = start_slot + i
                if slot_idx < len(timeline):
                    current_cpu, current_mem, current_io = timeline[slot_idx]
                    
                    req_cpu = cpu_slots[i] if i < len(cpu_slots) else 0.0
                    req_mem = memory_slots[i] if i < len(memory_slots) else 0.0
                    req_io = io_slots[i] if i < len(io_slots) else 0.0
                    
                    timeline[slot_idx] = (
                        current_cpu + req_cpu,
                        current_mem + req_mem,
                        current_io + req_io
                    )
        
        else:
            # Lambda/Athena: Update single resource slot array
            if self.service_type == ServiceType.LAMBDA:
                resource_slots = service_slots.get('instance_slots', [])
            else:  # ATHENA
                resource_slots = service_slots.get('query_slots', [])
            
            # Update resource timeline
            for i, resource_usage in enumerate(resource_slots):
                slot_idx = start_slot + i
                if slot_idx < len(timeline):
                    timeline[slot_idx] += resource_usage
        
        # Calculate total duration from slots
        if self.service_type == ServiceType.EC2:
            total_duration = max(len(cpu_slots), len(memory_slots), len(io_slots)) * self.time_slot_duration
        else:
            if self.service_type == ServiceType.LAMBDA:
                total_duration = len(service_slots.get('instance_slots', [])) * self.time_slot_duration
            else:  # ATHENA
                total_duration = len(service_slots.get('query_slots', [])) * self.time_slot_duration
        
        # Record scheduled query
        self.scheduled_queries.append((query.id, executor_name, start_time, total_duration))
        return True
    
    def get_resource_utilization(self) -> Dict[str, List[float]]:
        """Get resource utilization for each executor"""
        utilization = {}
        for executor_name, timeline in self.executor_timelines.items():
            exec_util = []
            for resource in timeline[:10]:  # Show first 10 slices
                if self.service_type == ServiceType.EC2:
                    cpu, mem, io = resource
                    max_util = max(cpu, mem, io)
                    exec_util.append(max_util)
                else:
                    util_percentage = resource / self.max_capacity if self.max_capacity > 0 else 0
                    exec_util.append(util_percentage)
            utilization[executor_name] = exec_util
        return utilization
    
    def calculate_resource_matching_score(self, query: Query, executor_name: str, vector_length: int = 10) -> float:
        """
        Calculate resource matching score between query and executor
        
        Score = <r, u> / |r|²
        where r is query resource demand vector, u is executor current usage vector
        
        Args:
            query: Query with precomputed resource_time_slots
            executor_name: Name of the executor
            vector_length: Length to pad vectors to
            
        Returns:
            Matching score (higher = better match)
        """
        if executor_name not in self.executor_timelines:
            return 0.0
            
        if not query.resource_time_slots:
            return 0.0
            
        # Get service-specific resource slots
        service_key = self.service_type.value
        if service_key not in query.resource_time_slots:
            return 0.0
            
        service_slots = query.resource_time_slots[service_key]
        timeline = self.executor_timelines[executor_name]
        
        # Create resource demand vector (r)
        if self.service_type == ServiceType.EC2:
            # For EC2, flatten CPU/Memory/IO slots into one expanded vector
            cpu_slots = service_slots.get('cpu_slots', [])
            memory_slots = service_slots.get('memory_slots', [])
            io_slots = service_slots.get('io_slots', [])
            
            # Create expanded demand vector: [cpu_0, cpu_1, ..., mem_0, mem_1, ..., io_0, io_1, ...]
            # Each resource type uses the full vector_length, resulting in 3 * vector_length total
            demand_vector = []
            
            # Add CPU values (full vector_length)
            for i in range(vector_length):
                cpu_val = cpu_slots[i] if i < len(cpu_slots) else 0.0
                demand_vector.append(cpu_val)
            
            # Add Memory values (full vector_length)
            for i in range(vector_length):
                mem_val = memory_slots[i] if i < len(memory_slots) else 0.0
                demand_vector.append(mem_val)
            
            # Add IO values (full vector_length)
            for i in range(vector_length):
                io_val = io_slots[i] if i < len(io_slots) else 0.0
                demand_vector.append(io_val)
        
        else:
            # For Lambda/Athena, use single resource slot array
            if self.service_type == ServiceType.LAMBDA:
                resource_slots = service_slots.get('instance_slots', [])
            else:  # ATHENA
                resource_slots = service_slots.get('query_slots', [])
            
            demand_vector = resource_slots[:vector_length]
        
        # Create current usage vector (u)
        usage_vector = []
        if self.service_type == ServiceType.EC2:
            # For EC2, create expanded usage vector matching the demand vector structure
            # Each resource type uses the full vector_length
            
            # Add CPU usage values (full vector_length)
            for i in range(vector_length):
                if i < len(timeline):
                    cpu, _, _ = timeline[i]
                    usage_vector.append(cpu)
                else:
                    usage_vector.append(0.0)
            
            # Add Memory usage values (full vector_length)
            for i in range(vector_length):
                if i < len(timeline):
                    _, mem, _ = timeline[i]
                    usage_vector.append(mem)
                else:
                    usage_vector.append(0.0)
            
            # Add IO usage values (full vector_length)
            for i in range(vector_length):
                if i < len(timeline):
                    _, _, io = timeline[i]
                    usage_vector.append(io)
                else:
                    usage_vector.append(0.0)
        else:
            # For Lambda/Athena, create normalized usage vector
            for i in range(min(len(timeline), vector_length)):
                usage_vector.append(float(timeline[i]) / self.max_capacity)
        
        # For EC2, vectors are already the correct length (3 * vector_length)
        # For Lambda/Athena, pad to vector_length
        if self.service_type != ServiceType.EC2:
            # Pad vectors to same length
            while len(demand_vector) < vector_length:
                demand_vector.append(0.0)
            while len(usage_vector) < vector_length:
                usage_vector.append(0.0)
        
        # Convert to numpy arrays for calculation
        r = np.array(demand_vector)
        u = np.array(usage_vector)
        
        # Calculate score: <r, u> / |r|²
        dot_product = np.dot(r, u)
        r_magnitude_squared = np.dot(r, r)
        
        if r_magnitude_squared == 0:
            return 0.0
            
        score = dot_product / r_magnitude_squared
        return float(score)
    
    def advance_time(self, elapsed_time: float):
        """Advance scheduler time, removing completed resource usage"""
        slots_to_remove = int(elapsed_time / self.time_slot_duration)
        if slots_to_remove > 0:
            # Remove completed time slots from all executor timelines
            for executor_name in self.executor_timelines.keys():
                timeline = self.executor_timelines[executor_name]
                self.executor_timelines[executor_name] = timeline[slots_to_remove:]
            
            # Update scheduled queries
            current_time = slots_to_remove * self.time_slot_duration
            remaining_queries = []
            for query_id, executor_name, start_time, duration in self.scheduled_queries:
                if start_time + duration > current_time:
                    remaining_queries.append((query_id, executor_name, max(0, start_time - current_time), duration))
            self.scheduled_queries = remaining_queries


class IntraScheduler:
    """
    Service-aware intra-scheduler with centralized scheduling logic
    """
    
    def __init__(self, services: Dict[ServiceType, List[BaseServiceExecutor]], queues: Dict[ServiceType, QueryQueue]):
        self.services = services
        self.queues = queues
        self.params = RUSHParameters()
        self.running = False
        
        # Create resource scheduler for each service type with their executors
        self.resource_schedulers: Dict[ServiceType, ResourceScheduler] = {}
        # Create scheduling events for each service type (for event-driven scheduling)
        self.schedule_events: Dict[ServiceType, asyncio.Event] = {}
        
        for service_type, executor_list in services.items():
            self.resource_schedulers[service_type] = ResourceScheduler(
                service_type, executor_list, self.params.TIME_SLOT_DURATION
            )
            self.schedule_events[service_type] = asyncio.Event()
        
        self.last_schedule_time = time.time()
    
    def trigger_scheduling(self, service_type: ServiceType):
        """Trigger scheduling for a specific service type"""
        if service_type in self.schedule_events:
            self.schedule_events[service_type].set()
    
    def trigger_all_scheduling(self):
        """Trigger scheduling for all service types"""
        for event in self.schedule_events.values():
            event.set()
    
    async def start_background_scheduling(self):
        """Start background service-specific scheduling"""
        self.running = True
        tasks = []
        for service_type in self.services.keys():
            task = asyncio.create_task(self._schedule_service_type(service_type))
            tasks.append(task)
        await asyncio.gather(*tasks)
    
    def stop_background_scheduling(self):
        """Stop background scheduling"""
        self.running = False
        # Trigger all events to wake up sleeping schedulers
        self.trigger_all_scheduling()
    
    async def _schedule_service_type(self, service_type: ServiceType):
        """Event-driven + timed background scheduling for specific service type"""
        while self.running:
            try:
                # Wait for either:
                # 1. A scheduling event (new query arrival or query completion)
                # 2. Timeout (regular timed scheduling)
                await asyncio.wait_for(
                    self.schedule_events[service_type].wait(), 
                    timeout=self.params.PROCESSING_CHECK_INTERVAL
                )
                # Clear the event for next trigger
                self.schedule_events[service_type].clear()
            except asyncio.TimeoutError:
                # Timeout occurred - this is normal, proceed with timed scheduling
                pass
            
            if not self.running:
                break
                
            # Advance scheduler time
            current_time = time.time()
            elapsed = current_time - self.last_schedule_time
            self.resource_schedulers[service_type].advance_time(elapsed)
            self.last_schedule_time = current_time
            
            # Try to schedule pending queries using resource matching scoring
            queue = self.queues[service_type]
            scheduler = self.resource_schedulers[service_type]
            scheduled_count = 0
            
            # Use while loop to dynamically recalculate scores after each scheduling
            # For now, assume single executor per service (first executor)
            if not self.services[service_type]:
                continue
                
            executor = self.services[service_type][0]  # Use first executor for single-executor logic
            
            # Initialize candidate queries - all waiting queries
            candidate_queries = list(queue.queue)
            
            while candidate_queries:
                # Calculate matching scores for all candidate queries
                best_query = None
                best_score = -float('inf')
                schedulable_queries = []  # Track which queries can still be scheduled
                
                for query in candidate_queries:
                    # Check if query can be scheduled on the executor
                    if scheduler.can_schedule_query_on_executor(query, executor.name, 0.0):
                        schedulable_queries.append(query)  # Keep for next iteration
                        score = scheduler.calculate_resource_matching_score(
                            query, executor.name, self.params.RESOURCE_VECTOR_LENGTH
                        )
                        if score > best_score:
                            best_score = score
                            best_query = query
                
                # Update candidate queries to only include schedulable ones for next iteration
                candidate_queries = schedulable_queries
                
                # If no query can be scheduled, break the loop
                if best_query is None:
                    print(f"[{service_type.value}] No more queries can be scheduled - {len(candidate_queries)} queries remain unschedulable")
                    break
                
                # Schedule the best matching query on the single executor
                if scheduler.schedule_query_on_executor(best_query, executor.name, 0.0):
                    # Remove this specific query from queue and candidate list
                    queue.remove_query(best_query)
                    candidate_queries.remove(best_query)  # Remove from candidates too
                    queue.add_to_processing(best_query)
                    asyncio.create_task(self._execute_query_background(executor, best_query, queue))
                    scheduled_count += 1
                    print(f"[{service_type.value}] Scheduled query {best_query.id} on {executor.name} (score: {best_score:.3f}) - {len(candidate_queries)} candidates remaining")
                else:
                    # Scheduling failed even though can_schedule returned True
                    # This shouldn't happen, but break to avoid infinite loop
                    print(f"[{service_type.value}] Warning: Failed to schedule query {best_query.id} despite positive feasibility check")
                    break
            
            # Log scheduling activity if any queries were scheduled
            if scheduled_count > 0:
                print(f"[{service_type.value}] Scheduled {scheduled_count} queries by matching score (single executor, optimized)")
    
    async def _execute_query_background(self, executor: BaseServiceExecutor, query: Query, queue: QueryQueue):
        """Execute query in background and handle result"""
        service_type = None
        # Find which service type this query belongs to
        for stype, squeue in self.queues.items():
            if squeue == queue:
                service_type = stype
                break
        
        try:
            result = await executor.execute_query(query)
            print(f"Query {query.id} completed on {executor.name} - Time: {result.execution_time:.2f}s, Cost: ${result.cost:.6f}")
        except Exception as e:
            print(f"Query {query.id} failed on {executor.name}: {e}")
        finally:
            queue.remove_from_processing(query)
            
            # Trigger scheduling when query completes (resource freed)
            if service_type:
                self.trigger_scheduling(service_type)
                print(f"[{service_type.value}] Query completion triggered scheduling")
    
    def get_resource_utilization(self) -> Dict[str, Dict[str, List[float]]]:
        """Get current resource utilization for each service and executor"""
        utilization = {}
        for service_type, scheduler in self.resource_schedulers.items():
            utilization[service_type.value] = scheduler.get_resource_utilization()
        return utilization
    
    def get_scheduler_info(self) -> Dict:
        """Get information about the service-specific schedulers"""
        return {
            "type": "service_specific_resource_aware",
            "description": "Service-specific resource-aware intra-scheduler",
            "features": [
                "EC2: CPU/Memory/IO resource modeling per instance",
                "Lambda: Function instance count modeling (shared pool)", 
                "Athena: Query concurrency modeling (shared pool)",
                "Per-executor resource tracking",
                "Load-based executor selection"
            ],
            "parameters": {
                "time_slice_duration": self.params.TIME_SLICE_DURATION,
                "ec2_max_resources": "CPU:1.0, Memory:1.0, IO:1.0",
                "lambda_max_instances": 100,
                "athena_max_concurrent_queries": 10
            },
            "current_utilization": {
                service_type.value: {
                    executor_name: util[:3] for executor_name, util in scheduler.get_resource_utilization().items()
                }
                for service_type, scheduler in self.resource_schedulers.items()
            }
        }
