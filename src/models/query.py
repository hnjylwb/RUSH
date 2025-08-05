from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
import time

class QueryType(Enum):
    SELECT = "SELECT"
    ANALYTICS = "ANALYTICS"
    AGGREGATION = "AGGREGATION"

class ServiceType(Enum):
    EC2 = "EC2"
    LAMBDA = "LAMBDA"
    ATHENA = "ATHENA"

class Query:
    def __init__(self, id: str, sql: str, query_type: QueryType, 
                 estimated_cost: Optional[float] = None, 
                 estimated_time: Optional[float] = None):
        self.id = id
        self.sql = sql
        self.query_type = query_type
        self.estimated_cost = estimated_cost
        self.estimated_time = estimated_time
        self.submit_time = time.time()  # Record submission time
        
        # Migration tracking to prevent ping-pong migrations
        self.migration_history: List[Tuple[float, str, str]] = []  # (timestamp, from_service, to_service)
        self.last_migration_time: Optional[float] = None
        self.migration_count = 0
        
        # Pipeline predictions from router (added for time-sliced scheduling)
        self.pipeline_predictions: Optional[List[Dict[str, Any]]] = None
        self.pipeline_data: Optional[List[Dict[str, Any]]] = None
        
        # Resource time slots for scheduling (precomputed by router)
        self.resource_time_slots: Optional[Dict[str, List[float]]] = None
        
        # Service-specific predictions from router
        self.service_predictions: Optional[Dict[str, Dict[str, float]]] = None  # {service: {time: x, cost: y}}
    
    def set_pipeline_info(self, predictions: List[Dict[str, Any]], pipeline_data: List[Dict[str, Any]]):
        """Set pipeline prediction information from router"""
        self.pipeline_predictions = predictions
        self.pipeline_data = pipeline_data
    
    def set_resource_time_slots(self, resource_time_slots: Dict[str, List[float]]):
        """Set precomputed resource time slots for scheduling"""
        self.resource_time_slots = resource_time_slots
    
    def set_service_predictions(self, service_predictions: Dict[str, Dict[str, float]]):
        """Set service-specific time and cost predictions from router"""
        self.service_predictions = service_predictions
    
    def get_predicted_latency(self, service_type: ServiceType) -> float:
        """Get predicted execution latency for given service type"""
        if self.service_predictions and service_type.value in self.service_predictions:
            return self.service_predictions[service_type.value].get('time', 1.0)
        return 1.0  # Default fallback
    
    def get_predicted_cost(self, service_type: ServiceType) -> float:
        """Get predicted execution cost for given service type"""
        if self.service_predictions and service_type.value in self.service_predictions:
            return self.service_predictions[service_type.value].get('cost', 0.001)
        return 0.001  # Default fallback
    
    def get_wait_time(self) -> float:
        """Get how long this query has been waiting in seconds"""
        return time.time() - self.submit_time
    
    def record_migration(self, from_service: str, to_service: str):
        """Record a migration event"""
        current_time = time.time()
        self.migration_history.append((current_time, from_service, to_service))
        self.last_migration_time = current_time
        self.migration_count += 1
    
    def time_since_last_migration(self) -> float:
        """Get time since last migration, or infinity if never migrated"""
        if self.last_migration_time is None:
            return float('inf')
        return time.time() - self.last_migration_time
    
    def has_recent_migration_to_service(self, service: str, window_seconds: float = 30.0) -> bool:
        """Check if query was recently migrated to this service (within window)"""
        current_time = time.time()
        for timestamp, from_service, to_service in reversed(self.migration_history):
            if current_time - timestamp > window_seconds:
                break
            if to_service == service:
                return True
        return False
    
    def __hash__(self):
        return hash(self.id)