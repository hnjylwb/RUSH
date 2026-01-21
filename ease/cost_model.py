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
    """
    execution_time: float
    cost: float


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

        Performance model: Sequential multi-stage execution
        Each stage: duration is fixed, actual time depends on resource capacity

        Args:
            resources: Query resource requirements (multi-stage)
            vm_config: VM configuration (cpu_cores, io_bandwidth_mbps, cost_per_hour, etc.)

        Returns:
            CostEstimate with time and cost
        """
        # === Performance Estimation ===

        io_bandwidth_mbps = vm_config['io_bandwidth_mbps']
        cpu_cores = vm_config['cpu_cores']

        # Estimate execution time for each stage
        total_time = 0.0

        for stage_idx in range(resources.num_stages):
            # CPU time: work = cpu_time, capacity = cpu_cores
            # Time needed = cpu_time / cpu_cores
            cpu_time_needed = resources.cpu_time[stage_idx] / cpu_cores

            # I/O time: work = data_scanned, capacity = io_bandwidth
            # Time needed = data_scanned / io_bandwidth
            data_scanned_mb = resources.data_scanned[stage_idx] / (1024 * 1024)
            io_time_needed = data_scanned_mb / io_bandwidth_mbps

            # Stage time is the max of CPU and I/O (they can overlap)
            stage_time = max(cpu_time_needed, io_time_needed)
            total_time += stage_time

        # === Cost Estimation ===

        cost_per_hour = vm_config['cost_per_hour']
        cost_per_second = cost_per_hour / 3600.0

        # Resource utilization factor: max of CPU and IO utilization across all stages
        max_cpu_util = max(resources.cpu_util)
        max_io_util = max(resources.io_util)
        resource_utilization = max(max_cpu_util, max_io_util)

        # Memory utilization factor
        memory_gb = resources.memory_required / (1024 ** 3)
        vm_memory_gb = vm_config['memory_gb']
        memory_utilization = min(1.0, memory_gb / vm_memory_gb)

        # Overall utilization: max of resource and memory utilization
        utilization = max(resource_utilization, memory_utilization)

        cost = total_time * cost_per_second * utilization

        return CostEstimate(
            execution_time=total_time,
            cost=cost
        )

    def estimate_faas(self, resources: ResourceRequirements,
                      faas_config: Dict) -> CostEstimate:
        """
        Estimate time and cost for FaaS execution

        Performance model: Parallel execution with instance selection
        Each stage: work is distributed across multiple instances

        Args:
            resources: Query resource requirements (multi-stage)
            faas_config: FaaS configuration (memory_sizes_gb list, cost_per_gb_second)

        Returns:
            CostEstimate with time and cost
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

        single_instance_cpu = selected_memory_gb * cpu_per_gb
        single_instance_io = selected_memory_gb * io_per_gb_mbps

        # Estimate number of parallel invocations
        num_partitions = 100  # Default partition count
        max_instances = min(100, int(selected_memory_gb * 10))
        instances_to_use = min(max_instances, num_partitions)

        # Total capacity across all instances
        total_cpu = single_instance_cpu * instances_to_use
        total_io = single_instance_io * instances_to_use

        # Estimate execution time for each stage
        total_time = 0.0

        for stage_idx in range(resources.num_stages):
            # CPU time: work = cpu_time, capacity = total_cpu
            # Time needed = cpu_time / total_cpu
            cpu_time_needed = resources.cpu_time[stage_idx] / total_cpu

            # I/O time: work = data_scanned, capacity = total_io
            # Time needed = data_scanned / total_io
            data_scanned_mb = resources.data_scanned[stage_idx] / (1024 * 1024)
            io_time_needed = data_scanned_mb / total_io

            # Stage time is the max of CPU and I/O
            stage_time = max(cpu_time_needed, io_time_needed)
            total_time += stage_time

        # === Cost Estimation ===

        cost_per_gb_second = faas_config['cost_per_gb_second']

        gb_seconds = total_time * selected_memory_gb * instances_to_use
        execution_cost = gb_seconds * cost_per_gb_second

        # Additional costs (S3, network, etc.)
        additional_cost = execution_cost * 0.1

        cost = execution_cost + additional_cost

        return CostEstimate(
            execution_time=total_time,
            cost=cost
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
            cost=cost
        )
