import asyncio
import random
import time
import boto3
import json
from typing import Dict, Any
from .base_executor import BaseServiceExecutor
from ..models.query import Query
from ..models.result import QueryResult

class LambdaExecutor(BaseServiceExecutor):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.memory_size = config.get('memory_size', 512)
        self.cost_per_invocation = config.get('cost_per_invocation', 0.0000002)
        
        # Real execution configuration
        if not self.simulation_mode:
            self.function_name = config.get('function_name', 'rush-sql-executor')
            self.region_name = config.get('region_name', 'us-east-1')
            
            # Initialize AWS Lambda client
            self.lambda_client = self._initialize_lambda_client()
        
    def _initialize_lambda_client(self):
        """Initialize AWS Lambda client"""
        try:
            # Use default AWS credentials from ~/.aws/credentials or IAM role
            return boto3.client('lambda', region_name=self.region_name)
        except Exception as e:
            print(f"[Lambda-{self.name}] Error initializing Lambda client: {e}")
            return None
    
    async def execute_query_real(self, query: Query) -> QueryResult:
        """Execute query using AWS Lambda function"""
        start_time = time.time()
        
        try:
            if not self.lambda_client or not self.function_name:
                raise Exception("Lambda client or function name not configured")
            
            # Prepare payload for Lambda function
            payload = {
                "query_id": query.id,
                "sql": query.sql,
                "query_type": query.query_type.value if query.query_type else "SELECT"
            }
            
            # Invoke Lambda function
            result_data = await self._invoke_lambda_function(payload)
            
            execution_time = time.time() - start_time
            
            # Calculate cost based on execution time and memory
            gb_seconds = (self.memory_size / 1024) * execution_time
            cost = self.cost_per_invocation + (gb_seconds * 0.0000166667)
            
            return QueryResult(
                query_id=query.id,
                service_used=f"Lambda-{self.memory_size}MB",
                result_data=result_data,
                execution_time=execution_time,
                cost=cost,
                status="SUCCESS"
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            gb_seconds = (self.memory_size / 1024) * execution_time
            cost = self.cost_per_invocation + (gb_seconds * 0.0000166667)
            
            return QueryResult(
                query_id=query.id,
                service_used=f"Lambda-{self.memory_size}MB",
                result_data={"error": str(e)},
                execution_time=execution_time,
                cost=cost,
                status="ERROR"
            )
    
    async def _invoke_lambda_function(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke AWS Lambda function with query payload"""
        try:
            # Invoke Lambda function synchronously
            response = self.lambda_client.invoke(
                FunctionName=self.function_name,
                InvocationType='RequestResponse',  # Synchronous invocation
                Payload=json.dumps(payload)
            )
            
            # Parse response
            status_code = response['StatusCode']
            
            if status_code != 200:
                raise Exception(f"Lambda invocation failed with status {status_code}")
            
            # Read and parse response payload
            response_payload = response['Payload'].read()
            
            if response.get('FunctionError'):
                error_info = json.loads(response_payload)
                raise Exception(f"Lambda function error: {error_info}")
            
            # Parse successful response
            result = json.loads(response_payload)
            
            return {
                "rows": result.get("rows", 0),
                "columns": result.get("columns", []),
                "data": result.get("data", []),
                "function_name": self.function_name,
                "memory_used": result.get("memory_used", self.memory_size),
                "duration": result.get("duration", 0)
            }
            
        except Exception as e:
            raise Exception(f"Lambda invocation error: {str(e)}")
    
    async def execute_query_simulation(self, query: Query) -> QueryResult:
        """Simulate Lambda executing simple query"""
        execution_time = random.uniform(0.5, 2.0)  # 0.5-2 seconds
        await asyncio.sleep(execution_time)
        
        gb_seconds = (self.memory_size / 1024) * execution_time
        cost = self.cost_per_invocation + (gb_seconds * 0.0000166667)
        
        return QueryResult(
            query_id=query.id,
            service_used=f"Lambda-{self.memory_size}MB",
            result_data={"rows": random.randint(10, 100), "columns": ["col1", "col2"]},
            execution_time=execution_time,
            cost=cost,
            status="SUCCESS"
        )
    
    def is_available(self) -> bool:
        if self.simulation_mode:
            return True
        
        # Check if Lambda function exists and is active
        if not self.lambda_client or not self.function_name:
            return False
        
        try:
            response = self.lambda_client.get_function(FunctionName=self.function_name)
            return response['Configuration']['State'] == 'Active'
        except Exception:
            return False