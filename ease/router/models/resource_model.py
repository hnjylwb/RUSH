"""
Resource model - 估算查询的资源需求
"""

from typing import Dict, Optional
from dataclasses import dataclass


@dataclass
class ResourceRequirements:
    """
    Query resource requirements

    Attributes:
        cpu_time: CPU time needed (seconds)
        data_scanned: Data to be scanned (bytes)
        scale_factor: Query complexity (1-100)
    """
    cpu_time: float
    data_scanned: float
    scale_factor: float = 50.0

    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary"""
        return {
            'cpu_time': self.cpu_time,
            'data_scanned': self.data_scanned,
            'scale_factor': self.scale_factor
        }


class ResourceModel:
    """
    Resource Model

    Estimates query resource requirements
    Supports multiple input sources: CSV, ML model, heuristics
    """

    def __init__(self, csv_path: Optional[str] = None):
        """
        Initialize resource model

        Args:
            csv_path: Optional path to CSV file with known resource requirements
        """
        self.csv_resources = {}
        if csv_path:
            self._load_csv(csv_path)

    def _load_csv(self, csv_path: str):
        """Load resource requirements from CSV"""
        import csv
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    query_id = row['query_id']
                    self.csv_resources[query_id] = ResourceRequirements(
                        cpu_time=float(row['cpu_time']),
                        data_scanned=float(row['data_scanned']),
                        scale_factor=float(row['scale_factor'])
                    )
        except Exception as e:
            print(f"Warning: Failed to load CSV: {e}")

    def estimate(self, query_id: str, sql: str) -> ResourceRequirements:
        """
        Estimate resource requirements

        Strategy:
        1. Check CSV first
        2. Use ML model if available
        3. Fall back to heuristics

        Args:
            query_id: Query identifier
            sql: SQL string

        Returns:
            ResourceRequirements object
        """
        # Strategy 1: CSV lookup
        if query_id in self.csv_resources:
            return self.csv_resources[query_id]

        # Strategy 2: ML model (TODO: implement)
        # requirements = self._ml_predict(sql)

        # Strategy 3: Heuristics
        return self._heuristic_estimate(sql)

    def _heuristic_estimate(self, sql: str) -> ResourceRequirements:
        """Simple heuristic estimation based on SQL"""
        sql_upper = sql.upper()

        # Simple heuristics
        has_join = 'JOIN' in sql_upper
        has_group = 'GROUP BY' in sql_upper
        has_order = 'ORDER BY' in sql_upper

        # Estimate complexity
        complexity = 1.0
        if has_join:
            complexity += 2.0
        if has_group:
            complexity += 1.0
        if has_order:
            complexity += 0.5

        return ResourceRequirements(
            cpu_time=complexity * 1.0,          # Base 1 second
            data_scanned=complexity * 50 * 1024 * 1024,  # Base 50MB
            scale_factor=min(100, complexity * 20)
        )
