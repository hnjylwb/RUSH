from typing import Any, Dict, Optional
from datetime import datetime

class QueryResult:
    def __init__(self, query_id: str, service_used: str,
                 result_data: Optional[Dict[str, Any]] = None,
                 execution_time: Optional[float] = None,
                 cost: Optional[float] = None,
                 status: str = "SUCCESS",
                 error_message: Optional[str] = None,
                 timestamp: datetime = None):
        self.query_id = query_id
        self.service_used = service_used
        self.result_data = result_data
        self.execution_time = execution_time
        self.cost = cost
        self.status = status
        self.error_message = error_message
        self.timestamp = timestamp or datetime.now()