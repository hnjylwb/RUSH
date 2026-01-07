"""
Router module for intelligent SQL query routing

This module implements a sophisticated routing system that:
1. Parses SQL queries into execution plans using DuckDB
2. Splits plans into pipelines based on pipeline breaker operators
3. Encodes pipelines into feature vectors
4. Predicts resource usage (CPU, memory, IO, duration) using XGBoost
5. Selects optimal service type based on predictions
"""

from .router import Router
from .query_parser import QueryParser
from .pipeline_splitter import PipelineSplitter
from .pipeline_encoder import PipelineEncoder
from .resource_predictor import ResourcePredictor

__all__ = [
    'Router',
    'QueryParser', 
    'PipelineSplitter',
    'PipelineEncoder',
    'ResourcePredictor'
]