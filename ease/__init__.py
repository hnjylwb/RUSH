"""
EASE - Elastic Analytics Service Executor (name subject to change)

A query scheduling system across heterogeneous cloud services
"""

from .core import Query, ServiceType, ServiceConfig, ExecutionResult, QueryType
from .executors import BaseExecutor, VMExecutor, FaaSExecutor, QaaSExecutor
from .router import Router
from .scheduler import IntraScheduler, InterScheduler
from .system import System
from .config import Config

__version__ = "0.1.0"

__all__ = [
    'Query', 'ServiceType', 'ServiceConfig', 'ExecutionResult', 'QueryType',
    'BaseExecutor', 'VMExecutor', 'FaaSExecutor', 'QaaSExecutor',
    'Router', 'IntraScheduler', 'InterScheduler',
    'System', 'Config'
]
