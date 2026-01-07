"""
Main system class that coordinates all components
"""

from typing import Dict, List, Optional
from .core import Query, ServiceType, ServiceConfig, ExecutionResult
from .executors import VMExecutor, FaaSExecutor, QaaSExecutor, BaseExecutor
from .router import Router
from .scheduler import IntraScheduler, InterScheduler
from .config import Config


class System:
    """
    Main system class

    Coordinates router, schedulers, and executors
    """

    def __init__(self, config_dir: str = "config",
                 resource_csv: Optional[str] = None):
        """
        Initialize system

        Args:
            config_dir: Configuration directory
            resource_csv: Path to query resource CSV
        """
        # Load configuration
        self.config = Config(config_dir)

        # Initialize executors
        self.executors = self._create_executors()

        # Initialize router
        service_configs = self._get_service_configs()
        self.router = Router(
            config=self.config.get('router', {}),
            service_configs=service_configs,
            resource_csv=resource_csv
        )

        # Initialize schedulers
        self.intra_scheduler = IntraScheduler(self.executors)
        self.inter_scheduler = InterScheduler(
            self.router,
            self.intra_scheduler,
            self.config.get('inter_scheduler', {})
        )

    def _create_executors(self) -> Dict[ServiceType, List[BaseExecutor]]:
        """Create executor instances based on configuration"""
        executors = {}

        # VM executors
        vm_configs = self.config.get('services.vm', [])
        executors[ServiceType.VM] = [
            VMExecutor(ServiceConfig(ServiceType.VM, cfg['name'], cfg))
            for cfg in vm_configs
        ]

        # FaaS executors
        faas_configs = self.config.get('services.faas', [])
        executors[ServiceType.FAAS] = [
            FaaSExecutor(ServiceConfig(ServiceType.FAAS, cfg['name'], cfg))
            for cfg in faas_configs
        ]

        # QaaS executors
        qaas_configs = self.config.get('services.qaas', [])
        executors[ServiceType.QAAS] = [
            QaaSExecutor(ServiceConfig(ServiceType.QAAS, cfg['name'], cfg))
            for cfg in qaas_configs
        ]

        return executors

    def _get_service_configs(self) -> Dict[ServiceType, Dict]:
        """Get aggregated service configurations for router"""
        configs = {}

        # Get first executor config for each service type (simplified)
        for service_type, executor_list in self.executors.items():
            if executor_list:
                configs[service_type] = executor_list[0].config.config

        return configs

    async def submit_query(self, query: Query) -> str:
        """
        Submit query to the system

        Args:
            query: Query to execute

        Returns:
            Query ID
        """
        # Step 1: Route query
        queue_sizes = {
            st: self.intra_scheduler.get_queue_size(st)
            for st in [ServiceType.VM, ServiceType.FAAS, ServiceType.QAAS]
        }
        decision = self.router.route(query, queue_sizes)

        # Step 2: Enqueue to selected service
        self.intra_scheduler.enqueue(query, decision.selected_service)

        print(f"Query {query.query_id} routed to {decision.selected_service.value}")
        print(f"  Estimated time: {query.estimated_time:.3f}s")
        print(f"  Estimated cost: ${query.estimated_cost:.6f}")

        return query.query_id

    def get_status(self) -> Dict:
        """Get system status"""
        return {
            'queues': {
                st.value: self.intra_scheduler.get_queue_size(st)
                for st in [ServiceType.VM, ServiceType.FAAS, ServiceType.QAAS]
            },
            'executors': {
                st.value: len(execs)
                for st, execs in self.executors.items()
            }
        }
