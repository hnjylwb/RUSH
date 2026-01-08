"""
Cost model - 估算查询在不同服务上的金钱成本
"""

from typing import Dict
from ...core import ResourceRequirements


class CostModel:
    """
    Cost Model

    Estimates query execution cost on different services
    """

    def __init__(self, config: Dict = None):
        """
        Initialize cost model

        Args:
            config: Model configuration
        """
        self.config = config or {}

    def estimate_vm(self, execution_time: float, vm_config: Dict,
                    resources: ResourceRequirements = None) -> float:
        """
        VM cost: time × hourly_rate × utilization

        Args:
            execution_time: Estimated execution time (seconds)
            vm_config: VM configuration
            resources: Resource requirements (for utilization estimation)

        Returns:
            Estimated cost in dollars
        """
        cost_per_hour = vm_config.get('cost_per_hour', 1.536)
        cost_per_second = cost_per_hour / 3600.0

        # Resource utilization factor
        if resources:
            utilization = 0.3 + (resources.scale_factor / 100) * 0.7
        else:
            utilization = 0.5

        return execution_time * cost_per_second * utilization

    def estimate_faas(self, execution_time: float, faas_config: Dict) -> float:
        """
        FaaS cost: gb_seconds × rate + additional costs

        Args:
            execution_time: Estimated execution time (seconds)
            faas_config: FaaS configuration

        Returns:
            Estimated cost in dollars
        """
        num_instances = faas_config.get('num_instances', 100)
        memory_per_instance_gb = faas_config.get('memory_per_instance_gb', 4)
        cost_per_gb_second = faas_config.get('cost_per_gb_second', 0.0000002)

        gb_seconds = execution_time * memory_per_instance_gb * num_instances
        execution_cost = gb_seconds * cost_per_gb_second

        # Additional costs (S3, network, etc.) - simplified
        additional_cost = execution_cost * 0.1

        return execution_cost + additional_cost

    def estimate_qaas(self, resources: ResourceRequirements, qaas_config: Dict) -> float:
        """
        QaaS cost: data_scanned × rate_per_tb

        Args:
            resources: Resource requirements
            qaas_config: QaaS configuration

        Returns:
            Estimated cost in dollars
        """
        data_scanned_tb = resources.data_scanned / (1024 ** 4)
        cost_per_tb = qaas_config.get('cost_per_tb', 5.0)

        return data_scanned_tb * cost_per_tb
