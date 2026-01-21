"""
Query scheduling server with HTTP API
"""

import asyncio
import uuid
from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime

from .core import Query, ServiceType, ServiceConfig, ExecutionResult, ExecutionStatus, ResourceRequirements
from .cost_model import CostModel
from .scheduler import Scheduler, Rescheduler
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

    def __init__(self, config_dir: str = "config",
                 host: str = "0.0.0.0", port: int = 8080):
        """
        Initialize scheduling server

        Args:
            config_dir: Configuration directory
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

        # Initialize cost model and schedulers
        service_configs = {
            service_type: executor.config.config
            for service_type, executor in self.executors.items()
        }
        self.cost_model = CostModel(config=self.config.get('cost_model', {}))

        # Convert executors dict to format expected by Scheduler
        executors_for_scheduler = {
            service_type: [executor]
            for service_type, executor in self.executors.items()
        }
        self.scheduler = Scheduler(executors=executors_for_scheduler)
        self.rescheduler = Rescheduler(
            cost_model=self.cost_model,
            scheduler=self.scheduler,
            service_configs=service_configs,
            config=self.config
        )

        # Task tracking
        self.tasks: Dict[str, QueryTask] = {}

        # Background tasks
        self.scheduler_task = None
        self.rescheduler_task = None
        self.health_check_task = None

    def _init_executors(self):
        """Initialize executors from configuration"""
        # Get enabled service types
        enabled_services = self.config.get('enabled_services', ['vm', 'faas', 'qaas'])
        enabled_set = set(enabled_services)

        # VM executors
        if 'vm' in enabled_set:
            vm_configs = self.config.get('services.vm', [])
            for vm_config in vm_configs:
                service_config = ServiceConfig(
                    service_type=ServiceType.VM,
                    name=vm_config.get('name', 'vm'),
                    config=vm_config
                )
                executor = VMExecutor(service_config)
                self.executors[ServiceType.VM] = executor

        # FaaS executors
        if 'faas' in enabled_set:
            faas_config = self.config.get('services.faas', {})
            if faas_config:
                # Create a single FaaS executor with memory sizes and pricing
                service_config = ServiceConfig(
                    service_type=ServiceType.FAAS,
                    name='faas',
                    config=faas_config
                )
                executor = FaaSExecutor(service_config)
                self.executors[ServiceType.FAAS] = executor

        # QaaS executors
        if 'qaas' in enabled_set:
            qaas_configs = self.config.get('services.qaas', [])
            for qaas_config in qaas_configs:
                service_config = ServiceConfig(
                    service_type=ServiceType.QAAS,
                    name=qaas_config.get('name', 'qaas'),
                    config=qaas_config
                )
                executor = QaaSExecutor(service_config)
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
                        sql=data['sql']
                    )

                    # Add resource profile if provided by client
                    if 'resource_profile' in data:
                        query.resource_requirements = data['resource_profile']

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

            # Worker auto-registration
            async def register_worker(request):
                """Auto-register a worker (assigns to first available VM executor)"""
                try:
                    data = await request.json()
                    endpoint = data.get('endpoint')

                    if not endpoint:
                        return web.json_response({
                            "error": "Missing 'endpoint'"
                        }, status=400)

                    # Find first VM executor and add worker
                    vm_executor = None
                    for service_type, executor in self.executors.items():
                        if service_type == ServiceType.VM:
                            vm_executor = executor
                            break

                    if not vm_executor:
                        return web.json_response({
                            "error": "No VM executor configured"
                        }, status=404)

                    # Add worker
                    added = vm_executor.add_worker(endpoint)

                    return web.json_response({
                        "status": "registered" if added else "already_registered",
                        "executor_name": vm_executor.name,
                        "endpoint": endpoint,
                        "total_workers": len(vm_executor.get_workers())
                    })

                except Exception as e:
                    return web.json_response({"error": str(e)}, status=400)

            async def deregister_worker(request):
                """Deregister a worker (called when worker shuts down)"""
                try:
                    data = await request.json()
                    endpoint = data.get('endpoint')

                    if not endpoint:
                        return web.json_response({
                            "error": "Missing 'endpoint'"
                        }, status=400)

                    # Find and remove worker from all VM executors
                    removed = False
                    for service_type, executor in self.executors.items():
                        if service_type == ServiceType.VM:
                            if executor.remove_worker(endpoint):
                                removed = True
                                break

                    if removed:
                        return web.json_response({
                            "status": "deregistered",
                            "endpoint": endpoint
                        })
                    else:
                        return web.json_response({
                            "status": "not_found",
                            "endpoint": endpoint
                        })

                except Exception as e:
                    return web.json_response({"error": str(e)}, status=400)

            app.router.add_get('/health', health)
            app.router.add_post('/submit', submit_query)
            app.router.add_get('/task/{task_id}', get_task)
            app.router.add_get('/status', get_status)

            # Worker management endpoints
            app.router.add_post('/workers/register', register_worker)
            app.router.add_post('/workers/deregister', deregister_worker)

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
            print("  Query Management:")
            print("    - POST /submit            - Submit query")
            print("    - GET  /task/{id}         - Get task status")
            print("  Worker Management:")
            print("    - POST /workers/register   - Register worker")
            print("    - POST /workers/deregister - Deregister worker")
            print("  Server Status:")
            print("    - GET  /health            - Health check")
            print("    - GET  /status            - Get server status")
        print()

        # Start background tasks
        self.scheduler_task = asyncio.create_task(self._run_scheduler())
        self.rescheduler_task = asyncio.create_task(self._run_rescheduler())
        self.health_check_task = asyncio.create_task(self._run_health_check())

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
        if self.rescheduler_task:
            self.rescheduler_task.cancel()
        if self.health_check_task:
            self.health_check_task.cancel()

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

        # Select service based on cost model estimates and queue sizes
        if not query.resource_requirements:
            # Default to VM if no resource requirements
            selected_service = ServiceType.VM
            estimated_time = 0.0
            estimated_cost = 0.0
        else:
            resources = ResourceRequirements(
                cpu_time=query.resource_requirements.get('cpu_time', 0),
                data_scanned=query.resource_requirements.get('data_scanned', 0),
                scale_factor=query.resource_requirements.get('scale_factor', 50.0)
            )

            # Get estimates for all services
            best_service = None
            best_score = float('inf')
            estimates = {}

            for service_type in self.executors.keys():
                service_config = self.executors[service_type].config.config

                # Get cost estimate
                if service_type == ServiceType.VM:
                    estimate = self.cost_model.estimate_vm(resources, service_config)
                elif service_type == ServiceType.FAAS:
                    estimate = self.cost_model.estimate_faas(resources, service_config)
                elif service_type == ServiceType.QAAS:
                    estimate = self.cost_model.estimate_qaas(resources, service_config)
                else:
                    continue

                estimates[service_type] = estimate

                # Calculate score: time + cost_weight * cost + load_penalty
                queue_size = self.scheduler.get_queue_size(service_type)
                cost_weight = 1.0
                load_weight = 0.1
                score = estimate.execution_time + cost_weight * estimate.cost + load_weight * queue_size

                if score < best_score:
                    best_score = score
                    best_service = service_type

            selected_service = best_service if best_service else ServiceType.VM
            estimated_time = estimates[selected_service].execution_time if selected_service in estimates else 0.0
            estimated_cost = estimates[selected_service].cost if selected_service in estimates else 0.0

        # Update query with routing decision
        query.routing_decision = selected_service.value
        query.estimated_cost = estimated_cost
        query.estimated_time = estimated_time

        # Create task
        task = QueryTask(
            task_id=task_id,
            query=query,
            client_id=client_id,
            submit_time=datetime.now()
        )
        self.tasks[task_id] = task

        # Enqueue to scheduler
        self.scheduler.enqueue(query, selected_service)

        print(f"[Client {client_id}] Query {query.query_id} to {selected_service.value} "
              f"(est. time: {estimated_time:.2f}s, cost: ${estimated_cost:.4f})")

        return task_id

    async def get_task_status(self, task_id: str) -> Optional[QueryTask]:
        """Get status of a submitted task"""
        return self.tasks.get(task_id)

    async def _run_scheduler(self):
        """Background task: run scheduler"""
        while self.running:
            try:
                # Schedule and execute queries for all service types
                results = await self.scheduler.schedule_and_execute_all()

                # Update task statuses based on results
                for service_results in results.values():
                    for result in service_results:
                        # Find the task for this query
                        for task in self.tasks.values():
                            if task.query.query_id == result.query_id:
                                task.status = result.status
                                task.result = result

                                status_str = "SUCCESS" if result.status == ExecutionStatus.SUCCESS else "FAILED"
                                print(f"[Scheduler] {status_str} Query {result.query_id} on {result.service_used} "
                                      f"(time: {result.execution_time:.2f}s, cost: ${result.cost:.4f})")
                                break

                await asyncio.sleep(0.1)  # Avoid busy waiting
            except Exception as e:
                print(f"[Scheduler] Error: {e}")

    async def _run_rescheduler(self):
        """Background task: run rescheduler"""
        while self.running:
            try:
                # Run rescheduler to check for migrations
                await self.rescheduler.check_and_migrate()
                await asyncio.sleep(1.0)  # Check every second
            except Exception as e:
                print(f"[Rescheduler] Error: {e}")

    async def _run_health_check(self):
        """Background task: check worker health"""
        while self.running:
            try:
                # Check health of all VM workers
                for service_type, executor in self.executors.items():
                    if service_type == ServiceType.VM:
                        for endpoint in executor.get_workers():
                            await executor.check_worker_health(endpoint)

                await asyncio.sleep(10.0)  # Check every 10 seconds
            except Exception as e:
                print(f"[HealthCheck] Error: {e}")

    def get_status(self) -> Dict:
        """Get server status"""
        queue_sizes = {
            service_type.value: self.scheduler.get_queue_size(service_type)
            for service_type in self.executors.keys()
        }

        executor_info = {}
        for service_type, executor in self.executors.items():
            info = executor.get_service_info()
            if service_type == ServiceType.VM:
                info['workers'] = {
                    'total': len(executor.get_workers()),
                    'healthy': len(executor.get_healthy_workers()),
                    'endpoints': {
                        ep: executor._worker_health.get(ep, {})
                        for ep in executor.get_workers()
                    }
                }
            executor_info[service_type.value] = info

        return {
            'running': self.running,
            'total_tasks': len(self.tasks),
            'queues': queue_sizes,
            'executors': executor_info
        }
