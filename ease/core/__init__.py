"""
Core data models and types
"""

from .query import Query
from .service import ServiceType, ServiceConfig
from .result import ExecutionResult

__all__ = ['Query', 'ServiceType', 'ServiceConfig', 'ExecutionResult']
