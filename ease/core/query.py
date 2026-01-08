"""
Query data model
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any


@dataclass
class ResourceRequirements:
    """
    Query resource requirements

    Resource requirements are provided by the client in JSON format.
    Used by the router to estimate execution time and cost on different services.

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


@dataclass
class Query:
    """
    Query representation for OLAP workloads

    Attributes:
        query_id: Unique identifier
        sql: SQL query string
        metadata: Additional metadata (user_id, priority, etc.)
    """
    query_id: str
    sql: str
    metadata: Optional[Dict[str, Any]] = None

    # Will be populated by router
    resource_requirements: Optional[Dict[str, float]] = None
    routing_decision: Optional[str] = None
    estimated_cost: Optional[float] = None
    estimated_time: Optional[float] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

