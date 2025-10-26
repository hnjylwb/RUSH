import uuid
import asyncio
from typing import Dict, List
from .models.query import Query, QueryType, ServiceType
from .models.result import QueryResult
from .executors.base_executor import BaseServiceExecutor
from .executors.ec2_executor import EC2Executor
from .executors.lambda_executor import LambdaExecutor
from .executors.athena_executor import AthenaExecutor
from .router.router import Router
from .scheduler.intra_scheduler import IntraScheduler
from .scheduler.inter_scheduler import InterScheduler
from .scheduler.query_queue import QueryQueue

class RUSHCore:
    def __init__(self):
        self.services: Dict[ServiceType, List[BaseServiceExecutor]] = {
            ServiceType.EC2: [
                EC2Executor({'name': 'EC2-1', 'instance_type': 'm5.8xlarge', 'cost_per_hour': 1.536}),
            ],
            ServiceType.LAMBDA: [
                LambdaExecutor({'name': 'Lambda-1', 'memory_size': 512}),
            ],
            ServiceType.ATHENA: [
                AthenaExecutor({'name': 'Athena-1', 'cost_per_tb': 5.0})
            ]
        }
        
        # Initialize queues for each service type
        self.queues: Dict[ServiceType, QueryQueue] = {
            service_type: QueryQueue(service_type) for service_type in self.services.keys()
        }
        
        self.router = Router()
        self.intra_scheduler = IntraScheduler(self.services, self.queues)
        self.inter_scheduler = InterScheduler(self.services, self.queues)
        
        # Start background schedulers
        self.scheduler_tasks = []
        
    async def start_background_schedulers(self):
        """Start background scheduling tasks"""
        intra_task = asyncio.create_task(self.intra_scheduler.start_background_scheduling())
        inter_task = asyncio.create_task(self.inter_scheduler.start_background_scheduling())
        self.scheduler_tasks = [intra_task, inter_task]
        
    def stop_background_schedulers(self):
        """Stop background scheduling tasks"""
        self.intra_scheduler.stop_background_scheduling()
        self.inter_scheduler.stop_background_scheduling()
        for task in self.scheduler_tasks:
            task.cancel()
    
    async def submit_query(self, sql: str) -> str:
        """Submit query and return query ID (non-blocking)"""
        # Create query object
        query = Query(
            id=str(uuid.uuid4()),
            sql=sql,
            query_type=self._classify_query(sql)
        )
        
        print(f"Submitting query {query.id}: {sql}")
        
        # Router determines the best service for this query
        selected_service_type = self.router.route_query(query, self.queues)
        print(f"Router selected service: {selected_service_type.value}")
        
        # Add query to the selected service queue
        selected_queue = self.queues[selected_service_type]
        selected_queue.add_query(query)
        print(f"Query {query.id} added to {selected_service_type.value} queue")
        
        # Trigger scheduling when new query arrives
        self.intra_scheduler.trigger_scheduling(selected_service_type)
        print(f"[{selected_service_type.value}] New query arrival triggered scheduling")
        
        return query.id
    
    def get_queue_status(self) -> Dict[str, Dict[str, int]]:
        """Get current queue status"""
        status = {}
        for service_type, queue in self.queues.items():
            status[service_type.value] = {
                "waiting": queue.get_queue_size(),
                "processing": queue.get_processing_count()
            }
        return status
    
    def _classify_query(self, sql: str) -> QueryType:
        sql_lower = sql.lower()
        if 'group by' in sql_lower or any(func in sql_lower for func in ['sum', 'count', 'avg']):
            return QueryType.AGGREGATION
        elif 'select' in sql_lower:
            return QueryType.SELECT
        else:
            return QueryType.ANALYTICS
    
    def get_resource_utilization(self) -> Dict[str, List[float]]:
        """Get current resource utilization for each service"""
        return self.intra_scheduler.get_resource_utilization()
