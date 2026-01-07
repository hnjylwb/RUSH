"""
Query scheduling server with HTTP API
"""

import asyncio
import uuid
import json
from typing import Dict, Optional
from dataclasses import dataclass, asdict
from datetime import datetime

from .core import Query, ServiceType, ExecutionResult, ExecutionStatus, QueryType
from .router import Router
from .scheduler import IntraScheduler, InterScheduler
from .executors import BaseExecutor, VMExecutor, FaaSExecutor, QaaSExecutor
from .config import Config


@dataclass
class QueryTask:
    """Query task submitted by client"""
    task_id: str
    query: Query
    client_id: str
    submit_time: datetime
    status: ExecutionStatus = ExecutionStatus.SUCCESS
    result: Optional[ExecutionResult] = None


class SchedulingServer:
    """
    Query scheduling server

    Continuously runs and serves multiple clients via HTTP API
    Manages executors, routes queries, and schedules execution
    """

    def __init__(self, config_dir: str = "config", resource_csv: str = None,
                 host: str = "0.0.0.0", port: int = 8080):
        """
        Initialize scheduling server

        Args:
            config_dir: Configuration directory
            resource_csv: Optional CSV file with query resource requirements
            host: Server host address
            port: Server port
        """
        self.config = Config(config_dir)
        self.host = host
        self.port = port
        self.running = False
        self.app = None
        self.runner = None

        # Initialize executors
        self.executors: Dict[ServiceType, BaseExecutor] = {}
        self._init_executors()

        # Initialize router and schedulers
        self.router = Router(
            services=self.executors,
            config=self.config,
            resource_csv=resource_csv
        )
        self.intra_scheduler = IntraScheduler()
        self.inter_scheduler = InterScheduler(
            intra_scheduler=self.intra_scheduler,
            router=self.router,
            config=self.config
        )

        # Task tracking
        self.tasks: Dict[str, QueryTask] = {}

        # Background tasks
        self.scheduler_task = None
        self.monitor_task = None

    def _init_executors(self):
        """Initialize executors from configuration"""
        # VM executors
        vm_configs = self.config.get('services.vm', [])
        for vm_config in vm_configs:
            executor = VMExecutor(vm_config)
            self.executors[ServiceType.VM] = executor

        # FaaS executors
        faas_configs = self.config.get('services.faas', [])
        for faas_config in faas_configs:
            executor = FaaSExecutor(faas_config)
            self.executors[ServiceType.FAAS] = executor

        # QaaS executors
        qaas_configs = self.config.get('services.qaas', [])
        for qaas_config in qaas_configs:
            executor = QaaSExecutor(qaas_config)
            self.executors[ServiceType.QAAS] = executor

    def _setup_routes(self):
        """Setup HTTP API routes"""
        try:
            from aiohttp import web

            app = web.Application()

            # Health check
            async def health(request):
                return web.json_response({"status": "ok", "running": self.running})

            # Submit query
            async def submit_query(request):
                try:
                    data = await request.json()

                    # Parse query from request
                    query = Query(
                        query_id=data['query_id'],
                        sql=data['sql'],
                        query_type=QueryType[data.get('query_type', 'OLAP')]
                    )
                    client_id = data.get('client_id', 'unknown')

                    # Submit query
                    task_id = await self.submit_query(query, client_id)

                    return web.json_response({
                        "task_id": task_id,
                        "status": "submitted"
                    })
                except Exception as e:
                    return web.json_response({"error": str(e)}, status=400)

            # Get task status
            async def get_task(request):
                task_id = request.match_info['task_id']
                task = await self.get_task_status(task_id)

                if task:
                    return web.json_response({
                        "task_id": task.task_id,
                        "query_id": task.query.query_id,
                        "client_id": task.client_id,
                        "status": task.status.value,
                        "submit_time": task.submit_time.isoformat()
                    })
                else:
                    return web.json_response({"error": "Task not found"}, status=404)

            # Get server status
            async def get_status(request):
                status = self.get_status()
                return web.json_response(status)

            app.router.add_get('/health', health)
            app.router.add_post('/submit', submit_query)
            app.router.add_get('/task/{task_id}', get_task)
            app.router.add_get('/status', get_status)

            self.app = app
            return True
        except ImportError:
            print("Warning: aiohttp not installed, HTTP API disabled")
            print("Server will run in standalone mode")
            return False

    async def start(self):
        """Start the scheduling server"""
        if self.running:
            print("Server is already running")
            return

        self.running = True
        print("=" * 80)
        print("Query Scheduling Server Started")
        print("=" * 80)
        print(f"Available executors:")
        for service_type, executor in self.executors.items():
            info = executor.get_service_info()
            print(f"  - {service_type.value}: {info}")

        # Try to setup HTTP API
        has_http = self._setup_routes()
        if has_http:
            print(f"\nHTTP API: http://{self.host}:{self.port}")
            print("  - GET  /health       - Health check")
            print("  - POST /submit       - Submit query")
            print("  - GET  /task/{id}    - Get task status")
            print("  - GET  /status       - Get server status")
        print()

        # Start background tasks
        self.scheduler_task = asyncio.create_task(self._run_scheduler())
        self.monitor_task = asyncio.create_task(self._run_monitor())

        # Start HTTP server if available
        if has_http:
            from aiohttp import web
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            site = web.TCPSite(self.runner, self.host, self.port)
            await site.start()

            print(f"Server is running on http://{self.host}:{self.port}")
            print("Press Ctrl+C to stop\n")

            # Keep running
            try:
                while self.running:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass
        else:
            # Run without HTTP API
            print("Server is running (no HTTP API)")
            print("Press Ctrl+C to stop\n")
            try:
                while self.running:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass

    async def stop(self):
        """Stop the scheduling server"""
        self.running = False

        # Cancel background tasks
        if self.scheduler_task:
            self.scheduler_task.cancel()
        if self.monitor_task:
            self.monitor_task.cancel()

        # Stop HTTP server
        if self.runner:
            await self.runner.cleanup()

        print("Server stopped")

    async def submit_query(self, query: Query, client_id: str) -> str:
        """
        Submit a query from a client

        Args:
            query: Query to execute
            client_id: Client identifier

        Returns:
            Task ID for tracking
        """
        # Generate task ID
        task_id = str(uuid.uuid4())

        # Route query
        queue_sizes = {
            service_type: self.intra_scheduler.get_queue_size(service_type)
            for service_type in self.executors.keys()
        }
        decision = self.router.route(query, queue_sizes)

        # Update query with routing decision
        query.routing_decision = decision.selected_service.value
        query.estimated_cost = decision.estimated_cost
        query.estimated_time = decision.estimated_time

        # Create task
        task = QueryTask(
            task_id=task_id,
            query=query,
            client_id=client_id,
            submit_time=datetime.now()
        )
        self.tasks[task_id] = task

        # Enqueue to scheduler
        self.intra_scheduler.enqueue(query, decision.selected_service)

        print(f"[Client {client_id}] Query {query.query_id} → {decision.selected_service.value} "
              f"(est. time: {decision.estimated_time:.2f}s, cost: ${decision.estimated_cost:.4f})")

        return task_id

    async def get_task_status(self, task_id: str) -> Optional[QueryTask]:
        """Get status of a submitted task"""
        return self.tasks.get(task_id)

    async def _run_scheduler(self):
        """Background task: process scheduled queries"""
        while self.running:
            try:
                # Check if there are queries to schedule
                for service_type in self.executors.keys():
                    if self.intra_scheduler.get_queue_size(service_type) > 0:
                        # Get next query from queue
                        queries = self.intra_scheduler.schedule(service_type, batch_size=1)
                        if queries:
                            query = queries[0]
                            # Execute query (simplified for now)
                            executor = self.executors[service_type]
                            print(f"[Scheduler] Executing {query.query_id} on {service_type.value}...")
                            # In real implementation, this would be async execution
                            # result = await executor.execute(query)

                await asyncio.sleep(0.1)  # Avoid busy waiting
            except Exception as e:
                print(f"[Scheduler] Error: {e}")

    async def _run_monitor(self):
        """Background task: monitor and optimize scheduling"""
        while self.running:
            try:
                # Run inter-scheduler to check for migrations
                await self.inter_scheduler.check_and_migrate()
                await asyncio.sleep(1.0)  # Check every second
            except Exception as e:
                print(f"[Monitor] Error: {e}")

    def get_status(self) -> Dict:
        """Get server status"""
        queue_sizes = {
            service_type.value: self.intra_scheduler.get_queue_size(service_type)
            for service_type in self.executors.keys()
        }

        return {
            'running': self.running,
            'total_tasks': len(self.tasks),
            'queues': queue_sizes,
            'executors': {
                service_type.value: executor.get_service_info()
                for service_type, executor in self.executors.items()
            }
        }
