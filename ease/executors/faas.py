"""
FaaS (Function as a Service) Executor

Example: AWS Lambda, Azure Functions, GCP Cloud Functions
"""

from typing import Dict
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

        # Cluster total capacity
        self.total_cpu = self.num_instances * self.cpu_per_instance
        self.total_io = self.num_instances * self.io_per_instance_mbps

    async def execute(self, query: Query) -> ExecutionResult:
        """Execute query on FaaS cluster"""
        # TODO: Implement actual execution
        return ExecutionResult(
            query_id=query.query_id,
            service_used=self.name,
            status=ExecutionStatus.SUCCESS,
            execution_time=0.0,
            cost=0.0
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
