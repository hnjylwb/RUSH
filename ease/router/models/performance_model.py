"""
Performance model - 估算查询在不同服务上的执行时间
"""

from typing import Dict
from dataclasses import dataclass
from .resource_model import ResourceRequirements


@dataclass
class PerformanceEstimate:
    """Performance estimation result"""
    execution_time: float       # seconds
    breakdown: Dict[str, float] # time breakdown (io, cpu, shuffle)


class PerformanceModel:
    """
    Performance Model

    Estimates query execution time on different services
    Based on resource requirements and service characteristics
    """

    def __init__(self, config: Dict = None):
        """
        Initialize performance model

        Args:
            config: Model configuration
        """
        self.config = config or {}

    def estimate_vm(self, resources: ResourceRequirements,
                    vm_config: Dict) -> PerformanceEstimate:
        """
        Estimate execution time on VM

        Model: Sequential execution (I/O → CPU → Shuffle)

        Args:
            resources: Query resource requirements
            vm_config: VM configuration (cpu_cores, io_bandwidth, etc.)

        Returns:
            PerformanceEstimate
        """
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

        total_time = io_time + cpu_time + shuffle_time

        return PerformanceEstimate(
            execution_time=total_time,
            breakdown={
                'io': io_time,
                'cpu': cpu_time,
                'shuffle': shuffle_time
            }
        )

    def estimate_faas(self, resources: ResourceRequirements,
                      faas_config: Dict) -> PerformanceEstimate:
        """
        Estimate execution time on FaaS cluster

        Model: Parallel execution

        Args:
            resources: Query resource requirements
            faas_config: FaaS configuration

        Returns:
            PerformanceEstimate
        """
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

        total_time = io_time + cpu_time + shuffle_time

        return PerformanceEstimate(
            execution_time=total_time,
            breakdown={
                'io': io_time,
                'cpu': cpu_time,
                'shuffle': shuffle_time
            }
        )

    def estimate_qaas(self, resources: ResourceRequirements,
                      qaas_config: Dict) -> PerformanceEstimate:
        """
        Estimate execution time on QaaS

        Model: Data-scan based with complexity factor

        Args:
            resources: Query resource requirements
            qaas_config: QaaS configuration

        Returns:
            PerformanceEstimate
        """
        # Base latency (cold start)
        base_latency = qaas_config.get('base_latency', 5.0)

        # Scan time
        data_scanned_tb = resources.data_scanned / (1024 ** 4)
        scan_speed = qaas_config.get('scan_speed_tb_per_sec', 0.5)
        scan_time = data_scanned_tb / scan_speed

        # Complexity factor
        complexity_factor = 1.0 + (resources.scale_factor / 100)

        total_time = (base_latency + scan_time) * complexity_factor

        return PerformanceEstimate(
            execution_time=total_time,
            breakdown={
                'base_latency': base_latency,
                'scan': scan_time,
                'complexity_factor': complexity_factor
            }
        )
