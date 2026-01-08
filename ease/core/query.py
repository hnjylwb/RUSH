"""
Query data model
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any


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

