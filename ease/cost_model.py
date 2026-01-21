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

        # VM execution cost
        execution_cost = total_time * cost_per_second * utilization

        # === S3 Access Cost (VM) ===
        # VM only has S3 GET requests for reading input data
        # AWS S3 pricing: $0.0004 per 1,000 GET requests

        s3_get_cost_per_1000 = vm_config.get('s3_get_cost_per_1000', 0.0004)
        s3_request_size_mb = vm_config.get('s3_request_size_mb', 64)  # Typical S3 object size

        # Calculate number of GET requests across all stages
        total_s3_get_requests = 0
        for stage_idx in range(resources.num_stages):
            data_scanned_mb = resources.data_scanned[stage_idx] / (1024 * 1024)
            num_requests = max(1, data_scanned_mb / s3_request_size_mb)
            total_s3_get_requests += num_requests

        s3_cost = (total_s3_get_requests / 1000.0) * s3_get_cost_per_1000

        cost = execution_cost + s3_cost

        return CostEstimate(
            execution_time=total_time,
            cost=cost
        )

    def estimate_faas(self, resources: ResourceRequirements,
                      faas_config: Dict) -> CostEstimate:
        """
        Estimate time and cost for FaaS execution

        Performance model: Parallel execution with optimized memory selection
        - Memory selection optimizes for both performance and cost
        - Each stage independently selects optimal memory configuration
        - Instance count based on 256 MB partitions per instance

        Lambda resource scaling:
        - CPU: 0-10 GB linear growth (~1.769 GB = 1 vCPU)
        - Network IO: 0-2 GB linear to 75 MB/s, 2-10 GB constant at 75 MB/s

        Args:
            resources: Query resource requirements (multi-stage)
            faas_config: FaaS configuration (memory_sizes_gb list, cost_per_gb_second)

        Returns:
            CostEstimate with time and cost
        """
        # === Lambda Resource Scaling Functions ===

        def get_cpu_cores(memory_gb: float) -> float:
            """Get CPU cores for given memory (1.769 GB = 1 vCPU)"""
            return memory_gb / 1.769 / 2

        def get_io_mbps(memory_gb: float) -> float:
            """Get network IO bandwidth in MB/s"""
            if memory_gb <= 2.0:
                # Linear: 0 GB → 0 MB/s, 2 GB → 75 MB/s
                return 37.5 * memory_gb
            else:
                # Saturated at 2 GB
                return 75.0

        def estimate_stage_time(memory_gb: float, stage_idx: int, num_instances: int) -> float:
            """Estimate execution time for a stage with given memory and instances"""
            # Per-instance capacity
            cpu_per_instance = get_cpu_cores(memory_gb)
            io_per_instance = get_io_mbps(memory_gb)

            # Total capacity across all instances
            total_cpu = cpu_per_instance * num_instances
            total_io = io_per_instance * num_instances

            # Time for CPU work
            cpu_time_needed = resources.cpu_time[stage_idx] / total_cpu

            # Time for IO work
            data_scanned_mb = resources.data_scanned[stage_idx] / (1024 * 1024)
            io_time_needed = data_scanned_mb / total_io

            # Stage time is bottleneck (max of CPU and IO)
            return max(cpu_time_needed, io_time_needed)

        # === Memory Selection and Execution ===

        memory_sizes = sorted(faas_config['memory_sizes_gb'])
        cost_per_gb_second = faas_config['cost_per_gb_second']

        # Minimum memory requirement (with 20% buffer)
        memory_required_gb = resources.memory_required / (1024 ** 3)
        min_memory_gb = max(0.128, memory_required_gb * 1.2)

        # Filter candidate memory sizes that meet minimum requirement
        candidate_memories = [m for m in memory_sizes if m >= min_memory_gb]
        if not candidate_memories:
            # If none meet requirement, use largest available
            candidate_memories = [memory_sizes[-1]]

        # Process each stage independently
        total_time = 0.0
        total_gb_seconds = 0.0

        for stage_idx in range(resources.num_stages):
            # Calculate number of instances (256 MB per instance)
            partition_size_mb = 256
            data_scanned_mb = resources.data_scanned[stage_idx] / (1024 * 1024)
            num_instances = max(1, int(data_scanned_mb / partition_size_mb))

            # Find optimal memory for this stage
            # Strategy: For Lambda's pricing model (time × memory × rate),
            # cost is often similar across memory sizes for CPU/IO-bound workloads.
            # Optimization goals:
            # 1. Minimize cost
            # 2. If costs are similar (within 5%), prefer faster execution
            # 3. For IO-bound: no benefit beyond 2 GB (IO saturates)
            best_memory = None
            best_time = float('inf')
            best_cost = float('inf')

            for memory_gb in candidate_memories:
                # Estimate time and cost for this memory configuration
                stage_time = estimate_stage_time(memory_gb, stage_idx, num_instances)
                stage_cost = stage_time * memory_gb * num_instances * cost_per_gb_second

                # Determine if IO-bound or CPU-bound
                data_scanned_mb = resources.data_scanned[stage_idx] / (1024 * 1024)
                io_time = (data_scanned_mb / num_instances) / get_io_mbps(memory_gb)
                cpu_time = resources.cpu_time[stage_idx] / (get_cpu_cores(memory_gb) * num_instances)
                is_io_bound = io_time >= cpu_time

                # Selection logic:
                # 1. If costs differ significantly (>5%), choose cheaper
                # 2. If costs similar, choose faster (lower time)
                # 3. For IO-bound workloads at 2+ GB, stop (IO saturated)

                cost_diff_ratio = abs(stage_cost - best_cost) / (best_cost + 1e-9)

                if stage_cost < best_cost * 0.95:
                    # Significantly cheaper
                    best_memory = memory_gb
                    best_time = stage_time
                    best_cost = stage_cost
                elif cost_diff_ratio < 0.05:
                    # Similar cost, prefer faster
                    if stage_time < best_time:
                        best_memory = memory_gb
                        best_time = stage_time
                        best_cost = stage_cost

                # Early stopping for IO-bound workloads
                if is_io_bound and memory_gb >= 2.0:
                    # IO saturated, no benefit from more memory
                    break

            # Use the selected optimal memory for this stage
            total_time += best_time
            total_gb_seconds += best_time * best_memory * num_instances

        # === Cost Estimation ===

        execution_cost = total_gb_seconds * cost_per_gb_second

        # === S3 Access Cost (FaaS) ===
        # FaaS has both GET (read input) and PUT (write shuffle) costs
        # AWS S3 pricing:
        #   GET: $0.0004 per 1,000 requests
        #   PUT: $0.005 per 1,000 requests

        s3_get_cost_per_1000 = faas_config.get('s3_get_cost_per_1000', 0.0004)
        s3_put_cost_per_1000 = faas_config.get('s3_put_cost_per_1000', 0.005)
        s3_request_size_mb = faas_config.get('s3_request_size_mb', 64)  # Typical S3 object size

        total_s3_get_requests = 0
        total_s3_put_requests = 0

        for stage_idx in range(resources.num_stages):
            data_scanned_mb = resources.data_scanned[stage_idx] / (1024 * 1024)

            # GET requests: reading input data
            num_get_requests = max(1, data_scanned_mb / s3_request_size_mb)
            total_s3_get_requests += num_get_requests

            # PUT requests: writing shuffle data (if not the last stage)
            # Assume each stage writes intermediate results equal to data scanned
            # Last stage typically writes final results to client, not S3
            if stage_idx < resources.num_stages - 1:
                num_put_requests = max(1, data_scanned_mb / s3_request_size_mb)
                total_s3_put_requests += num_put_requests

        s3_get_cost = (total_s3_get_requests / 1000.0) * s3_get_cost_per_1000
        s3_put_cost = (total_s3_put_requests / 1000.0) * s3_put_cost_per_1000
        s3_cost = s3_get_cost + s3_put_cost

        cost = execution_cost + s3_cost

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
