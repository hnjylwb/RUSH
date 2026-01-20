"""
Cost Model
"""

from typing import Dict
from dataclasses import dataclass
from .core import ResourceRequirements


@dataclass
class CostEstimate:
    """
    Cost estimation result for a query on a specific service

    Attributes:
        execution_time: Estimated execution time (seconds)
        cost: Estimated cost (dollars)
        breakdown: Detailed breakdown of time components
    """
    execution_time: float
    cost: float
    breakdown: Dict[str, float]


class CostModel:
    """
    Cost Model

    Estimates query execution time and cost on different services.
    Combines performance modeling and cost calculation.
    """

    def __init__(self, config: Dict = None):
        """
        Initialize cost model

        Args:
            config: Model configuration
        """
        self.config = config or {}

    def estimate_vm(self, resources: ResourceRequirements,
                    vm_config: Dict) -> CostEstimate:
        """
        Estimate time and cost for VM execution

        Performance model: Sequential execution (I/O → CPU → Shuffle)
        Cost model: time × hourly_rate × utilization

        Args:
            resources: Query resource requirements
            vm_config: VM configuration (cpu_cores, io_bandwidth, cost_per_hour, etc.)

        Returns:
            CostEstimate with time, cost, and breakdown
        """
        # === Performance Estimation ===

        # I/O time
        io_bandwidth_mbs = vm_config.get('io_bandwidth_mbps', 1250)
        data_scanned_mb = resources.data_scanned / (1024 * 1024)
        io_time = data_scanned_mb / io_bandwidth_mbs

        # CPU time (given)
        cpu_time = resources.cpu_time

        # Shuffle time (based on scale_factor)
        shuffle_ratio = 0.1 + (resources.scale_factor / 100) * 0.25
        shuffle_data_mb = data_scanned_mb * shuffle_ratio
        network_bandwidth_mbs = vm_config.get('network_bandwidth_mbps', 1250)
        shuffle_time = shuffle_data_mb / network_bandwidth_mbs

        execution_time = io_time + cpu_time + shuffle_time

        # === Cost Estimation ===

        cost_per_hour = vm_config.get('cost_per_hour', 1.536)
        cost_per_second = cost_per_hour / 3600.0

        # Resource utilization factor
        utilization = 0.3 + (resources.scale_factor / 100) * 0.7

        cost = execution_time * cost_per_second * utilization

        return CostEstimate(
            execution_time=execution_time,
            cost=cost,
            breakdown={
                'io': io_time,
                'cpu': cpu_time,
                'shuffle': shuffle_time,
                'utilization': utilization
            }
        )

    def estimate_faas(self, resources: ResourceRequirements,
                      faas_config: Dict) -> CostEstimate:
        """
        Estimate time and cost for FaaS cluster execution

        Performance model: Parallel execution
        Cost model: gb_seconds × rate + additional costs

        Args:
            resources: Query resource requirements
            faas_config: FaaS configuration (num_instances, memory, costs, etc.)

        Returns:
            CostEstimate with time, cost, and breakdown
        """
        # === Performance Estimation ===

        # Cluster capacity
        total_cpu = faas_config.get('total_cpu', 120)
        total_io = faas_config.get('total_io', 7500)

        # I/O time (parallel)
        data_scanned_mb = resources.data_scanned / (1024 * 1024)
        io_time = data_scanned_mb / total_io

        # CPU time (parallel)
        cpu_time = resources.cpu_time / total_cpu

        # Shuffle time
        shuffle_ratio = 0.1 + (resources.scale_factor / 100) * 0.25
        shuffle_data_mb = data_scanned_mb * shuffle_ratio
        shuffle_time = shuffle_data_mb / total_io * 2  # write + read

        execution_time = io_time + cpu_time + shuffle_time

        # === Cost Estimation ===

        num_instances = faas_config.get('num_instances', 100)
        memory_per_instance_gb = faas_config.get('memory_per_instance_gb', 4)
        cost_per_gb_second = faas_config.get('cost_per_gb_second', 0.0000002)

        gb_seconds = execution_time * memory_per_instance_gb * num_instances
        execution_cost = gb_seconds * cost_per_gb_second

        # Additional costs (S3, network, etc.) - simplified
        additional_cost = execution_cost * 0.1

        cost = execution_cost + additional_cost

        return CostEstimate(
            execution_time=execution_time,
            cost=cost,
            breakdown={
                'io': io_time,
                'cpu': cpu_time,
                'shuffle': shuffle_time,
                'execution_cost': execution_cost,
                'additional_cost': additional_cost
            }
        )

    def estimate_qaas(self, resources: ResourceRequirements,
                      qaas_config: Dict) -> CostEstimate:
        """
        Estimate time and cost for QaaS execution

        Performance model: Data-scan based with complexity factor
        Cost model: data_scanned × rate_per_tb

        Args:
            resources: Query resource requirements
            qaas_config: QaaS configuration (scan_speed, cost_per_tb, etc.)

        Returns:
            CostEstimate with time, cost, and breakdown
        """
        # === Performance Estimation ===

        # Base latency (cold start)
        base_latency = qaas_config.get('base_latency', 5.0)

        # Scan time
        data_scanned_tb = resources.data_scanned / (1024 ** 4)
        scan_speed = qaas_config.get('scan_speed_tb_per_sec', 0.5)
        scan_time = data_scanned_tb / scan_speed

        # Complexity factor
        complexity_factor = 1.0 + (resources.scale_factor / 100)

        execution_time = (base_latency + scan_time) * complexity_factor

        # === Cost Estimation ===

        cost_per_tb = qaas_config.get('cost_per_tb', 5.0)
        cost = data_scanned_tb * cost_per_tb

        return CostEstimate(
            execution_time=execution_time,
            cost=cost,
            breakdown={
                'base_latency': base_latency,
                'scan': scan_time,
                'complexity_factor': complexity_factor,
                'data_scanned_tb': data_scanned_tb
            }
        )
