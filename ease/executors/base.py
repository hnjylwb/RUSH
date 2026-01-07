"""
Base executor interface
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from ..core import Query, ExecutionResult, ServiceConfig


class BaseExecutor(ABC):
    """
    Base class for all executors

    Defines the interface that VM/FaaS/QaaS executors must implement
    """

    def __init__(self, config: ServiceConfig):
        """
        Initialize executor

        Args:
            config: Service configuration
        """
        self.config = config
        self.service_type = config.service_type
        self.name = config.name

    @abstractmethod
    async def execute(self, query: Query) -> ExecutionResult:
        """
        Execute a query

        Args:
            query: Query to execute

        Returns:
            ExecutionResult with timing, cost, and result data
        """
        pass

    @abstractmethod
    def estimate_cost(self, query: Query, estimated_time: float) -> float:
        """
        Estimate execution cost

        Args:
            query: Query to estimate
            estimated_time: Estimated execution time (seconds)

        Returns:
            Estimated cost in dollars
        """
        pass

    @abstractmethod
    def get_capacity(self) -> Dict[str, float]:
        """
        Get current available capacity

        Returns:
            Dict with capacity metrics (cpu, memory, io, etc.)
        """
        pass

    def get_service_info(self) -> Dict[str, Any]:
        """Get service information"""
        return {
            'service_type': self.service_type.value,
            'name': self.name,
            'capacity': self.get_capacity()
        }
