from enum import Enum

class ServiceStatus(Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    ERROR = "ERROR"

class Service:
    def __init__(self, name: str, service_type: str, 
                 status: ServiceStatus = ServiceStatus.IDLE,
                 current_load: float = 0.0, capacity: float = 100.0,
                 cost_per_hour: float = 0.0):
        self.name = name
        self.service_type = service_type
        self.status = status
        self.current_load = current_load
        self.capacity = capacity
        self.cost_per_hour = cost_per_hour
    
    def is_available(self) -> bool:
        return self.status == ServiceStatus.IDLE and self.current_load < self.capacity