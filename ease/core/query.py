"""
Query data model
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any, List


@dataclass
class ResourceRequirements:
    """
    Query resource requirements for multi-stage OLAP queries

    Resource requirements are provided by the client in JSON format.
    Used by the cost model to estimate execution time and cost on different services.

    OLAP queries typically execute in multiple stages (e.g., scan, join, aggregate).
    Each stage has different CPU and I/O requirements.

    Attributes:
        cpu_time: CPU time per stage (seconds), list for multi-stage queries
        data_scanned: Data scanned per stage (bytes), list for multi-stage queries
        memory_required: Peak memory requirement (bytes), single value for entire query
    """
    cpu_time: List[float]  # CPU time for each stage
    data_scanned: List[float]  # Data scanned for each stage
    memory_required: float  # Peak memory requirement in bytes

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'cpu_time': self.cpu_time,
            'data_scanned': self.data_scanned,
            'memory_required': self.memory_required
        }

    @property
    def total_cpu_time(self) -> float:
        """Total CPU time across all stages"""
        return sum(self.cpu_time)

    @property
    def total_data_scanned(self) -> float:
        """Total data scanned across all stages"""
        return sum(self.data_scanned)

    @property
    def num_stages(self) -> int:
        """Number of execution stages"""
        return len(self.cpu_time)


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

    resource_requirements: Optional[Dict[str, float]] = None
    routing_decision: Optional[str] = None
    estimated_cost: Optional[float] = None
    estimated_time: Optional[float] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

