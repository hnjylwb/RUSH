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

        Performance model: Sequential multi-stage execution (I/O → CPU per stage)
        Cost model: time × hourly_rate × utilization

        Args:
            resources: Query resource requirements (multi-stage)
            vm_config: VM configuration (cpu_cores, io_bandwidth, cost_per_hour, etc.)

        Returns:
            CostEstimate with time, cost, and breakdown
        """
        # === Performance Estimation ===

        io_bandwidth_mbs = vm_config['io_bandwidth_mbps']
        network_bandwidth_mbs = vm_config['network_bandwidth_mbps']

        # Estimate execution time for each stage
        stage_times = []
        total_io_time = 0.0
        total_cpu_time = 0.0
        total_shuffle_time = 0.0

        for stage_idx in range(resources.num_stages):
            # I/O time for this stage
            data_scanned_mb = resources.data_scanned[stage_idx] / (1024 * 1024)
            io_time = data_scanned_mb / io_bandwidth_mbs

            # CPU time for this stage (given)
            cpu_time = resources.cpu_time[stage_idx]

            # Shuffle time (data transfer between stages)
            # Estimate shuffle data as a fraction of scanned data
            shuffle_ratio = 0.1 + (stage_idx / resources.num_stages) * 0.2
            shuffle_data_mb = data_scanned_mb * shuffle_ratio
            shuffle_time = shuffle_data_mb / network_bandwidth_mbs

            stage_time = io_time + cpu_time + shuffle_time
            stage_times.append(stage_time)

            total_io_time += io_time
            total_cpu_time += cpu_time
            total_shuffle_time += shuffle_time

        # Sequential execution across stages
        execution_time = sum(stage_times)

        # === Cost Estimation ===

        cost_per_hour = vm_config['cost_per_hour']
        cost_per_second = cost_per_hour / 3600.0

        # Resource utilization factor (based on memory usage)
        memory_gb = resources.memory_required / (1024 ** 3)
        vm_memory_gb = vm_config['memory_gb']
        utilization = min(1.0, 0.3 + (memory_gb / vm_memory_gb) * 0.7)

        cost = execution_time * cost_per_second * utilization

        return CostEstimate(
            execution_time=execution_time,
            cost=cost,
            breakdown={
                'io': total_io_time,
                'cpu': total_cpu_time,
                'shuffle': total_shuffle_time,
                'utilization': utilization,
                'num_stages': resources.num_stages,
                'stage_times': stage_times
            }
        )

    def estimate_faas(self, resources: ResourceRequirements,
                      faas_config: Dict) -> CostEstimate:
        """
        Estimate time and cost for FaaS execution

        Performance model: Parallel execution across stages with instance selection
        Cost model: gb_seconds × rate

        Args:
            resources: Query resource requirements (multi-stage)
            faas_config: FaaS configuration (memory_sizes_gb list, cost_per_gb_second)

        Returns:
            CostEstimate with time, cost, and breakdown
        """
        # === Select appropriate Lambda instance ===

        memory_sizes = sorted(faas_config['memory_sizes_gb'])

        # Select based on memory requirement
        memory_required_gb = resources.memory_required / (1024 ** 3)
        required_memory_gb = max(2, memory_required_gb * 1.2)  # 20% buffer

        # Select smallest memory size that meets requirement
        selected_memory_gb = None
        for mem_gb in memory_sizes:
            if mem_gb >= required_memory_gb:
                selected_memory_gb = mem_gb
                break

        # If no size is large enough, use largest
        if selected_memory_gb is None:
            selected_memory_gb = memory_sizes[-1]

        # === Performance Estimation ===

        # Lambda CPU and IO scale with memory
        cpu_per_gb = 1.0 / 1.8
        io_per_gb_mbps = 75

        total_cpu = selected_memory_gb * cpu_per_gb
        total_io = selected_memory_gb * io_per_gb_mbps

        # Estimate number of parallel invocations
        num_partitions = 100  # Default partition count
        max_instances = min(100, int(selected_memory_gb * 10))
        instances_to_use = min(max_instances, num_partitions)

        # Adjust for parallelism
        total_cpu *= instances_to_use
        total_io *= instances_to_use

        # Estimate execution time for each stage
        stage_times = []
        total_io_time = 0.0
        total_cpu_time = 0.0
        total_shuffle_time = 0.0

        for stage_idx in range(resources.num_stages):
            # I/O time (parallel)
            data_scanned_mb = resources.data_scanned[stage_idx] / (1024 * 1024)
            io_time = data_scanned_mb / total_io

            # CPU time (parallel)
            cpu_time = resources.cpu_time[stage_idx] / total_cpu

            # Shuffle time (data transfer between stages)
            shuffle_ratio = 0.1 + (stage_idx / resources.num_stages) * 0.2
            shuffle_data_mb = data_scanned_mb * shuffle_ratio
            shuffle_time = shuffle_data_mb / total_io * 2  # write + read

            stage_time = io_time + cpu_time + shuffle_time
            stage_times.append(stage_time)

            total_io_time += io_time
            total_cpu_time += cpu_time
            total_shuffle_time += shuffle_time

        # Parallel execution across stages
        execution_time = sum(stage_times)

        # === Cost Estimation ===

        cost_per_gb_second = faas_config['cost_per_gb_second']

        gb_seconds = execution_time * selected_memory_gb * instances_to_use
        execution_cost = gb_seconds * cost_per_gb_second

        # Additional costs (S3, network, etc.)
        additional_cost = execution_cost * 0.1

        cost = execution_cost + additional_cost

        return CostEstimate(
            execution_time=execution_time,
            cost=cost,
            breakdown={
                'io': total_io_time,
                'cpu': total_cpu_time,
                'shuffle': total_shuffle_time,
                'execution_cost': execution_cost,
                'additional_cost': additional_cost,
                'memory_gb': selected_memory_gb,
                'instances_to_use': instances_to_use,
                'num_stages': resources.num_stages,
                'stage_times': stage_times
            }
        )

    def estimate_qaas(self, resources: ResourceRequirements,
                      qaas_config: Dict) -> CostEstimate:
        """
        Estimate time and cost for QaaS execution

        Performance model: Data-scan based with complexity factor
        Cost model: data_scanned × rate_per_tb

        Args:
            resources: Query resource requirements (multi-stage)
            qaas_config: QaaS configuration (scan_speed, cost_per_tb, etc.)

        Returns:
            CostEstimate with time, cost, and breakdown
        """
        # === Performance Estimation ===

        base_latency = qaas_config['base_latency']
        scan_speed = qaas_config['scan_speed_tb_per_sec']
        total_data_scanned_tb = resources.total_data_scanned / (1024 ** 4)
        scan_time = total_data_scanned_tb / scan_speed

        # Complexity factor (based on number of stages and CPU requirements)
        complexity_factor = 1.0 + (resources.num_stages / 10) + (resources.total_cpu_time / 100)

        execution_time = (base_latency + scan_time) * complexity_factor

        # === Cost Estimation ===

        cost_per_tb = qaas_config['cost_per_tb']
        cost = total_data_scanned_tb * cost_per_tb

        return CostEstimate(
            execution_time=execution_time,
            cost=cost,
            breakdown={
                'base_latency': base_latency,
                'scan': scan_time,
                'complexity_factor': complexity_factor,
                'data_scanned_tb': total_data_scanned_tb,
                'num_stages': resources.num_stages
            }
        )
