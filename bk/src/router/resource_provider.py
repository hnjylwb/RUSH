"""
Resource Provider - 统一的资源需求提供接口

支持多种资源需求来源：
1. CSV 文件（优先）
2. ML 模型预测（回退）
"""

from typing import Dict, Optional
from dataclasses import dataclass
import csv
import os


@dataclass
class QueryResources:
    """查询的资源需求"""
    cpu_time: float         # CPU 时间 (秒)
    data_scanned: float     # 扫描数据量 (字节)
    scale_factor: float     # 查询复杂度 (1-100)

    def calculate_shuffle_data(self) -> float:
        """
        计算 Shuffle 数据量
        基于 scale_factor 在 10%-35% 之间线性插值
        """
        if self.scale_factor <= 1:
            return 0.1 * self.data_scanned
        elif self.scale_factor >= 100:
            return 0.35 * self.data_scanned
        else:
            percentage = 0.1 + ((self.scale_factor - 1) / 99) * 0.25
            return percentage * self.data_scanned


class ResourceProvider:
    """
    资源需求提供者

    策略：优先使用 CSV 中的已知资源需求，如果没有则使用 ML 模型预测
    """

    def __init__(self, csv_path: Optional[str] = None, use_ml_fallback: bool = True):
        """
        Args:
            csv_path: CSV 文件路径（可选）
            use_ml_fallback: 如果 CSV 中找不到，是否使用 ML 模型预测
        """
        self.csv_resources = {}
        self.use_ml_fallback = use_ml_fallback
        self.ml_predictor = None

        # 加载 CSV 资源（如果提供）
        if csv_path and os.path.exists(csv_path):
            self._load_csv_resources(csv_path)
            print(f"Loaded {len(self.csv_resources)} query resources from {csv_path}")

        # 初始化 ML 预测器（如果需要）
        if use_ml_fallback:
            self._init_ml_predictor()

    def _load_csv_resources(self, csv_path: str) -> None:
        """
        从 CSV 文件加载资源需求

        CSV 格式：
        query_id,cpu_time,data_scanned,scale_factor
        Q1,1.196,38890000,50
        Q2,0.176,10000000,25
        """
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    query_id = row['query_id']
                    self.csv_resources[query_id] = QueryResources(
                        cpu_time=float(row['cpu_time']),
                        data_scanned=float(row['data_scanned']),
                        scale_factor=float(row['scale_factor'])
                    )
        except Exception as e:
            print(f"Warning: Failed to load CSV resources from {csv_path}: {e}")

    def _init_ml_predictor(self) -> None:
        """初始化 ML 预测器（保留原有的 ResourcePredictor）"""
        try:
            from .resource_predictor import ResourcePredictor
            self.ml_predictor = ResourcePredictor()
            print("ML predictor initialized as fallback")
        except Exception as e:
            print(f"Warning: Failed to initialize ML predictor: {e}")
            self.ml_predictor = None

    def get_resources(self, query_id: str, query_sql: str = None,
                     execution_plan: Dict = None,
                     pipeline_predictions: list = None) -> QueryResources:
        """
        获取查询的资源需求

        策略：
        1. 优先从 CSV 中查找
        2. 如果没有且启用了 ML，使用 ML 模型预测
        3. 否则返回默认值

        Args:
            query_id: 查询 ID
            query_sql: 查询 SQL（用于 ML 预测）
            execution_plan: 执行计划（用于 ML 预测）
            pipeline_predictions: 管道级预测结果（用于转换）

        Returns:
            QueryResources 对象
        """
        # 策略 1: 从 CSV 查找
        if query_id in self.csv_resources:
            resources = self.csv_resources[query_id]
            print(f"Using CSV resources for query {query_id}")
            return resources

        # 策略 2: 使用 ML 模型预测
        if self.use_ml_fallback and pipeline_predictions:
            resources = self._predict_from_pipelines(pipeline_predictions)
            print(f"Using ML predicted resources for query {query_id}")
            return resources

        # 策略 3: 返回默认值
        print(f"Warning: No resources found for query {query_id}, using defaults")
        return self._get_default_resources()

    def _predict_from_pipelines(self, pipeline_predictions: list) -> QueryResources:
        """
        从管道级预测结果转换为资源需求

        Args:
            pipeline_predictions: 原有的管道级预测结果
                [{'cpu_usage': 0.5, 'memory_usage': 0.5, 'io_usage': 0.3, 'duration': 2.0}, ...]

        Returns:
            QueryResources 对象
        """
        # CPU 时间：所有管道的 duration 之和
        total_cpu_time = sum(pred['duration'] for pred in pipeline_predictions)

        # 数据扫描量：基于 IO usage 估算
        # 假设 IO usage 0.1 ≈ 100 MB
        total_io_usage = sum(pred['io_usage'] for pred in pipeline_predictions)
        estimated_data_scanned = total_io_usage * 1024 * 1024 * 1024  # 转换为字节

        # Scale factor：基于管道数量和复杂度
        num_pipelines = len(pipeline_predictions)
        avg_cpu_usage = sum(pred['cpu_usage'] for pred in pipeline_predictions) / max(num_pipelines, 1)

        # 简单启发式：管道越多、CPU 使用越高，scale_factor 越大
        scale_factor = min(100, max(1, num_pipelines * 10 + avg_cpu_usage * 20))

        return QueryResources(
            cpu_time=total_cpu_time,
            data_scanned=estimated_data_scanned,
            scale_factor=scale_factor
        )

    def _get_default_resources(self) -> QueryResources:
        """返回默认的资源需求"""
        return QueryResources(
            cpu_time=2.0,           # 2 秒 CPU 时间
            data_scanned=100 * 1024 * 1024,  # 100 MB
            scale_factor=50.0       # 中等复杂度
        )

    def add_resource(self, query_id: str, resources: QueryResources) -> None:
        """
        动态添加资源需求（用于运行时更新）

        Args:
            query_id: 查询 ID
            resources: 资源需求
        """
        self.csv_resources[query_id] = resources
        print(f"Added resources for query {query_id}")
