"""
VM (Virtual Machine) Executor

Example: AWS EC2, Azure VM, GCP Compute Engine
"""

from typing import Dict
from .base import BaseExecutor
from ..core import Query, ExecutionResult, ExecutionStatus, ServiceConfig


class VMExecutor(BaseExecutor):
    """
    VM Executor

    Characteristics:
    - Fixed resources (CPU, memory, IO)
    - Persistent instance
    - Billing by time
    """

    def __init__(self, config: ServiceConfig):
        super().__init__(config)

        # VM-specific configuration
        self.cpu_cores = config.config.get('cpu_cores', 32)
        self.memory_gb = config.config.get('memory_gb', 128)
        self.io_bandwidth_mbps = config.config.get('io_bandwidth_mbps', 1250)
        self.cost_per_hour = config.config.get('cost_per_hour', 1.536)

    async def execute(self, query: Query) -> ExecutionResult:
        """Execute query on VM"""
        # TODO: Implement actual execution
        # For now, return a placeholder
        return ExecutionResult(
            query_id=query.query_id,
            service_used=self.name,
            status=ExecutionStatus.SUCCESS,
            execution_time=0.0,
            cost=0.0
        )

    def estimate_cost(self, query: Query, estimated_time: float) -> float:
        """
        VM cost model: time × hourly_rate

        Args:
            query: Query object
            estimated_time: Estimated execution time (seconds)

        Returns:
            Estimated cost in dollars
        """
        cost_per_second = self.cost_per_hour / 3600.0
        return estimated_time * cost_per_second

    def get_capacity(self) -> Dict[str, float]:
        """Get VM capacity"""
        return {
            'cpu_cores': self.cpu_cores,
            'memory_gb': self.memory_gb,
            'io_bandwidth_mbps': self.io_bandwidth_mbps
        }
