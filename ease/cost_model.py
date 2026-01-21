"""
Cost Model
"""

from typing import Dict
from dataclasses import dataclass
import pickle
from pathlib import Path
from .core import ResourceRequirements
from .feature_extractor import QaaSFeatureExtractor


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

        # Initialize feature extractors
        self.qaas_feature_extractor = QaaSFeatureExtractor(config)

        # Load QaaS regression model if available
        self.qaas_model = None
        self.qaas_model_metadata = {}
        qaas_model_path = self.config.get('qaas_model_path')
        if qaas_model_path:
            model_path = Path(qaas_model_path)
            if model_path.exists():
                try:
                    with open(model_path, 'rb') as f:
                        model_data = pickle.load(f)

                    # Extract model and metadata
                    if isinstance(model_data, dict):
                        self.qaas_model = model_data.get('model')
                        self.qaas_model_metadata = {
                            'feature_names': model_data.get('feature_names', []),
                            'model_type': model_data.get('model_type', 'Unknown')
                        }
                        print(f"Loaded {self.qaas_model_metadata['model_type']} from {model_path}")
                        print(f"Expected {len(self.qaas_model_metadata['feature_names'])} features")
                    else:
                        # Legacy: model saved directly without metadata
                        self.qaas_model = model_data
                        print(f"Loaded model from {model_path} (legacy format)")

                except Exception as e:
                    print(f"Warning: Failed to load QaaS model: {e}")
            else:
                print(f"Warning: QaaS model path does not exist: {model_path}")

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
                      qaas_config: Dict, sql: str = None) -> CostEstimate:
        """
        Estimate time and cost for QaaS execution

        Performance model: Regression model based on query features
        Cost model: data_scanned * rate_per_tb

        Args:
            resources: Query resource requirements (multi-stage)
            qaas_config: QaaS configuration (cost_per_tb, etc.)
            sql: Optional SQL query string for plan-based feature extraction

        Returns:
            CostEstimate with time and cost

        Raises:
            RuntimeError: If QaaS model is not loaded
        """
        # === Performance Estimation ===

        if self.qaas_model is None:
            raise RuntimeError(
                "QaaS model not loaded. Please provide 'qaas_model_path' in config "
                "and ensure the model file exists."
            )

        # Extract features using the feature extractor (with SQL if provided)
        features = self.qaas_feature_extractor.extract_features(resources, sql)

        # Predict execution time using the model
        execution_time = self.qaas_model.predict([features])[0]

        # === Cost Estimation ===

        cost_per_tb = qaas_config['cost_per_tb']
        total_data_scanned_tb = resources.total_data_scanned / (1024 ** 4)
        cost = total_data_scanned_tb * cost_per_tb

        return CostEstimate(
            execution_time=execution_time,
            cost=cost
        )
