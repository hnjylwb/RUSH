"""
Detailed Cost Model - 借鉴 CloudGlide 的执行建模方式

将查询执行分解为三个阶段：
1. I/O 阶段：数据扫描
2. CPU 阶段：计算处理
3. Shuffle 阶段：跨管道数据传输（多管道查询）

为每个服务类型（EC2/Lambda/Athena）提供详细的时间和成本估算
"""

from typing import Tuple, Dict
from ..models.query import ServiceType
from .resource_provider import QueryResources


class DetailedCostModel:
    """
    详细成本模型

    基于 CloudGlide 的思路，将查询执行分解为 I/O、CPU、Shuffle 三个阶段
    """

    def __init__(self, config: Dict = None):
        """
        Args:
            config: 服务配置参数（可选）
        """
        self.config = config or {}

        # 默认服务配置（来自 CloudGlide 和 AWS 定价）
        self._init_default_configs()

    def _init_default_configs(self):
        """初始化默认配置"""
        # EC2 配置 (m5.8xlarge)
        self.ec2_config = {
            'cpu_cores': 32,
            'memory_gb': 128,
            'io_bandwidth_mbps': 10000,      # 10 Gbps = 1250 MB/s
            'network_bandwidth_mbps': 10000,  # 10 Gbps
            'cost_per_hour': 1.536,
            'io_bandwidth_mbs': 1250,         # MB/s
            'network_bandwidth_mbs': 1250     # MB/s
        }

        # Lambda 配置 (100 instances × 4GB)
        self.lambda_config = {
            'num_instances': 100,
            'memory_per_instance_gb': 4,
            'cpu_per_instance_cores': 1.2,    # vCPU cores per instance
            'io_per_instance_mbps': 75,       # MB/s per instance
            'cost_per_gb_second': 0.0000002,  # $0.0000002 per GB-second
            # S3 costs
            's3_read_cost_per_1000': 0.0004,  # $0.0004 per 1000 GET requests
            's3_write_cost_per_1000': 0.005,  # $0.005 per 1000 PUT requests
        }

        # 计算集群总容量
        self.lambda_config['total_cpu_cores'] = (
            self.lambda_config['num_instances'] *
            self.lambda_config['cpu_per_instance_cores']
        )  # 120 cores
        self.lambda_config['total_io_mbs'] = (
            self.lambda_config['num_instances'] *
            self.lambda_config['io_per_instance_mbps']
        )  # 7500 MB/s

        # Athena 配置
        self.athena_config = {
            'cost_per_tb': 5.0,               # $5 per TB scanned
            'base_latency_seconds': 5.0,      # 基础启动延迟
            'scan_speed_tb_per_second': 0.5,  # 扫描速度（每秒处理 TB）
            'complexity_factor_per_join': 0.2,  # 每个 JOIN 增加 20% 时间
            'complexity_factor_per_agg': 0.15   # 每个聚合增加 15% 时间
        }

    def estimate_execution(
        self,
        service_type: ServiceType,
        resources: QueryResources,
        num_joins: int = 0,
        num_aggregations: int = 0
    ) -> Tuple[float, float, Dict]:
        """
        估算查询在指定服务上的执行时间和成本

        Args:
            service_type: 服务类型
            resources: 查询资源需求
            num_joins: JOIN 操作数量（用于复杂度估算）
            num_aggregations: 聚合操作数量

        Returns:
            (execution_time, execution_cost, breakdown)
            - execution_time: 执行时间（秒）
            - execution_cost: 执行成本（美元）
            - breakdown: 详细分解信息
        """
        if service_type == ServiceType.EC2:
            return self._estimate_ec2(resources)
        elif service_type == ServiceType.LAMBDA:
            return self._estimate_lambda(resources)
        elif service_type == ServiceType.ATHENA:
            return self._estimate_athena(resources, num_joins, num_aggregations)
        else:
            raise ValueError(f"Unsupported service type: {service_type}")

    def _estimate_ec2(self, resources: QueryResources) -> Tuple[float, float, Dict]:
        """
        EC2 执行估算

        模型：三阶段串行执行
        - I/O 阶段：data_scanned / io_bandwidth
        - CPU 阶段：cpu_time（已给定）
        - Shuffle 阶段：shuffle_data / network_bandwidth

        成本：execution_time × cost_per_second × resource_utilization
        """
        config = self.ec2_config

        # 计算 Shuffle 数据量
        shuffle_data = resources.calculate_shuffle_data()

        # I/O 时间
        data_scanned_mb = resources.data_scanned / (1024 * 1024)
        io_time = data_scanned_mb / config['io_bandwidth_mbs']

        # CPU 时间（直接使用给定值）
        cpu_time = resources.cpu_time

        # Shuffle 时间
        shuffle_data_mb = shuffle_data / (1024 * 1024)
        shuffle_time = shuffle_data_mb / config['network_bandwidth_mbs']

        # 总时间（三阶段串行）
        total_time = io_time + cpu_time + shuffle_time

        # 成本计算
        cost_per_second = config['cost_per_hour'] / 3600.0

        # 资源利用率：基于 scale_factor（复杂度越高，利用率越高）
        # scale_factor: 1-100 → resource_util: 0.3-1.0
        resource_utilization = 0.3 + (resources.scale_factor / 100.0) * 0.7

        total_cost = total_time * cost_per_second * resource_utilization

        breakdown = {
            'io_time': io_time,
            'cpu_time': cpu_time,
            'shuffle_time': shuffle_time,
            'total_time': total_time,
            'resource_utilization': resource_utilization,
            'cost_per_second': cost_per_second,
            'total_cost': total_cost
        }

        return total_time, total_cost, breakdown

    def _estimate_lambda(self, resources: QueryResources) -> Tuple[float, float, Dict]:
        """
        Lambda 执行估算

        模型：集群并行执行（100 个 4GB 实例）
        - I/O 阶段：data_scanned / cluster_io_capacity
        - CPU 阶段：cpu_time / cluster_cpu_capacity
        - Shuffle 阶段：(shuffle_data / cluster_io_capacity) × 2  # 写 + 读

        成本：
        1. 执行成本：total_time × memory × instances × cost_per_gb_second
        2. S3 访问成本：(reads + writes) / 1000 × unit_cost
        """
        config = self.lambda_config

        # 计算 Shuffle 数据量
        shuffle_data = resources.calculate_shuffle_data()

        # I/O 时间（集群并行）
        data_scanned_mb = resources.data_scanned / (1024 * 1024)
        io_time = data_scanned_mb / config['total_io_mbs']

        # CPU 时间（集群并行）
        cpu_time = resources.cpu_time / config['total_cpu_cores']

        # Shuffle 时间（写 + 读，通过 S3）
        shuffle_data_mb = shuffle_data / (1024 * 1024)
        shuffle_write_time = shuffle_data_mb / config['total_io_mbs']
        shuffle_read_time = shuffle_data_mb / config['total_io_mbs']
        shuffle_time = shuffle_write_time + shuffle_read_time

        # 总时间
        total_time = io_time + cpu_time + shuffle_time

        # 成本计算
        # 1. Lambda 执行成本（GB-seconds）
        total_gb_seconds = (
            total_time *
            config['memory_per_instance_gb'] *
            config['num_instances']
        )
        execution_cost = total_gb_seconds * config['cost_per_gb_second']

        # 2. S3 访问成本
        # 假设每次 Shuffle 涉及 N×N 次读写（N = num_instances）
        if shuffle_data > 0:
            num_shuffles = max(1, int(resources.scale_factor / 20))  # 基于复杂度估算
            s3_writes = num_shuffles * config['num_instances'] * config['num_instances']
            s3_reads = s3_writes
        else:
            s3_writes = 0
            s3_reads = 0

        s3_write_cost = (s3_writes / 1000.0) * config['s3_write_cost_per_1000']
        s3_read_cost = (s3_reads / 1000.0) * config['s3_read_cost_per_1000']
        s3_total_cost = s3_write_cost + s3_read_cost

        # 总成本
        total_cost = execution_cost + s3_total_cost

        breakdown = {
            'io_time': io_time,
            'cpu_time': cpu_time,
            'shuffle_write_time': shuffle_write_time,
            'shuffle_read_time': shuffle_read_time,
            'shuffle_time': shuffle_time,
            'total_time': total_time,
            'gb_seconds': total_gb_seconds,
            'execution_cost': execution_cost,
            's3_writes': s3_writes,
            's3_reads': s3_reads,
            's3_write_cost': s3_write_cost,
            's3_read_cost': s3_read_cost,
            's3_total_cost': s3_total_cost,
            'total_cost': total_cost
        }

        return total_time, total_cost, breakdown

    def _estimate_athena(
        self,
        resources: QueryResources,
        num_joins: int = 0,
        num_aggregations: int = 0
    ) -> Tuple[float, float, Dict]:
        """
        Athena 执行估算

        模型：基于数据扫描量的启发式
        - 基础时间：base_latency（冷启动）
        - 扫描时间：data_scanned_tb / scan_speed
        - 复杂度调整：(1 + num_joins × factor + num_aggs × factor)

        成本：data_scanned_tb × $5/TB
        """
        config = self.athena_config

        # 数据扫描量（TB）
        data_scanned_tb = resources.data_scanned / (1024 ** 4)

        # 基础时间
        base_time = config['base_latency_seconds']

        # 扫描时间
        scan_time = data_scanned_tb / config['scan_speed_tb_per_second']

        # 复杂度因子
        complexity_factor = 1.0
        complexity_factor += num_joins * config['complexity_factor_per_join']
        complexity_factor += num_aggregations * config['complexity_factor_per_agg']

        # 如果没有提供 JOIN/AGG 数量，基于 scale_factor 估算
        if num_joins == 0 and num_aggregations == 0:
            # scale_factor: 1-100 → complexity: 1.0-2.0
            complexity_factor = 1.0 + (resources.scale_factor / 100.0)

        # 总时间
        total_time = (base_time + scan_time) * complexity_factor

        # 成本（仅基于数据扫描量）
        total_cost = data_scanned_tb * config['cost_per_tb']

        breakdown = {
            'base_time': base_time,
            'scan_time': scan_time,
            'complexity_factor': complexity_factor,
            'total_time': total_time,
            'data_scanned_tb': data_scanned_tb,
            'cost_per_tb': config['cost_per_tb'],
            'total_cost': total_cost
        }

        return total_time, total_cost, breakdown

    def update_config(self, service_type: ServiceType, config_updates: Dict):
        """
        更新服务配置（用于实验调参）

        Args:
            service_type: 服务类型
            config_updates: 要更新的配置项
        """
        if service_type == ServiceType.EC2:
            self.ec2_config.update(config_updates)
        elif service_type == ServiceType.LAMBDA:
            self.lambda_config.update(config_updates)
            # 重新计算集群总容量
            self.lambda_config['total_cpu_cores'] = (
                self.lambda_config['num_instances'] *
                self.lambda_config['cpu_per_instance_cores']
            )
            self.lambda_config['total_io_mbs'] = (
                self.lambda_config['num_instances'] *
                self.lambda_config['io_per_instance_mbps']
            )
        elif service_type == ServiceType.ATHENA:
            self.athena_config.update(config_updates)

    def get_config(self, service_type: ServiceType) -> Dict:
        """获取服务配置"""
        if service_type == ServiceType.EC2:
            return self.ec2_config.copy()
        elif service_type == ServiceType.LAMBDA:
            return self.lambda_config.copy()
        elif service_type == ServiceType.ATHENA:
            return self.athena_config.copy()
        else:
            return {}
