import asyncio
from collections import deque
from typing import Dict, List
from ..models.query import Query, ServiceType
from ..models.result import QueryResult

class QueryQueue:
    def __init__(self, service_type: ServiceType):
        self.service_type = service_type
        self.queue = deque()
        self.processing = []
        
    def add_query(self, query: Query):
        """Add query to waiting queue"""
        self.queue.append(query)
        
    def get_next_query(self) -> Query:
        """Get next query from queue"""
        if self.queue:
            return self.queue.popleft()
        return None
        
    def add_to_processing(self, query: Query):
        """Mark query as being processed"""
        self.processing.append(query)
        
    def remove_from_processing(self, query: Query):
        """Remove query from processing list"""
        if query in self.processing:
            self.processing.remove(query)
            
    def remove_query(self, query: Query):
        """Remove specific query from waiting queue"""
        if query in self.queue:
            self.queue.remove(query)
    
    def get_queue_size(self) -> int:
        return len(self.queue)
        
    def get_processing_count(self) -> int:
        return len(self.processing)