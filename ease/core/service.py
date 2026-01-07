"""
Service types and configurations
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any


class ServiceType(Enum):
    """Service type enumeration"""
    VM = "vm"       # Virtual Machine (e.g., EC2)
    FAAS = "faas"   # Function as a Service (e.g., Lambda)
    QAAS = "qaas"   # Query as a Service (e.g., Athena)


@dataclass
class ServiceConfig:
    """
    Service configuration

    Stores configuration for a specific service instance
    """
    service_type: ServiceType
    name: str
    config: Dict[str, Any]

    # Common fields
    @property
    def cost_per_hour(self) -> float:
        """Cost per hour (for VM/FaaS billing)"""
        return self.config.get('cost_per_hour', 0.0)

    @property
    def capacity(self) -> Dict[str, float]:
        """Service capacity (cpu, memory, io, etc.)"""
        return self.config.get('capacity', {})


# Service type display names
SERVICE_NAMES = {
    ServiceType.VM: "Virtual Machine",
    ServiceType.FAAS: "FaaS",
    ServiceType.QAAS: "Query Service"
}
