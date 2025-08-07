from abc import ABC, abstractmethod
from typing import Dict, Any
from ..models.query import Query
from ..models.result import QueryResult

class BaseServiceExecutor(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.name = config.get('name', 'Unknown')
        from ..config.parameters import RUSHParameters
        params = RUSHParameters()
        self.simulation_mode = config.get('simulation_mode', params.SIMULATION_MODE)  # Config can override global setting
        
    async def execute_query(self, query: Query) -> QueryResult:
        """Main execution method that chooses between real and simulation"""
        if self.simulation_mode:
            return await self.execute_query_simulation(query)
        else:
            return await self.execute_query_real(query)
    
    @abstractmethod
    async def execute_query_real(self, query: Query) -> QueryResult:
        """Real execution method to be implemented by subclasses"""
        pass
    
    @abstractmethod
    async def execute_query_simulation(self, query: Query) -> QueryResult:
        """Simulation execution method to be implemented by subclasses"""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        pass
