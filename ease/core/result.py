"""
Execution result model
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from enum import Enum


class ExecutionStatus(Enum):
    """Execution status"""
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class ExecutionResult:
    """
    Query execution result

    Attributes:
        query_id: Query identifier
        service_used: Which service executed the query
        status: Execution status
        execution_time: Actual execution time (seconds)
        cost: Actual cost (dollars)
        result_data: Query result data
        error: Error message (if failed)
    """
    query_id: str
    service_used: str
    status: ExecutionStatus
    execution_time: float
    cost: float
    result_data: Optional[Any] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
