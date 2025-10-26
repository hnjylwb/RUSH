import asyncio
import random
import time
import boto3
import aiohttp
from typing import Dict, Any
from .base_executor import BaseServiceExecutor
from ..models.query import Query
from ..models.result import QueryResult

class EC2Executor(BaseServiceExecutor):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.instance_type = config.get('instance_type', 't3.medium')
        self.cost_per_hour = config.get('cost_per_hour', 0.0416)
        
        # Real execution configuration
        if not self.simulation_mode:
            self.instance_id = config.get('instance_id')
            self.api_endpoint = config.get('api_endpoint')  # HTTP API endpoint on EC2
            self.api_key = config.get('api_key')  # API key for authentication
            
            # Initialize AWS EC2 client
            self.ec2_client = self._initialize_ec2_client()
        
    def _initialize_ec2_client(self):
        """Initialize AWS EC2 client"""
        try:
            # Use default AWS credentials from ~/.aws/credentials or IAM role
            return boto3.client('ec2')
        except Exception as e:
            print(f"[EC2-{self.name}] Error initializing EC2 client: {e}")
            return None
    
    async def execute_query_real(self, query: Query) -> QueryResult:
        """Execute SQL query on EC2 instance via HTTP API"""
        start_time = time.time()
        
        try:
            if not self.api_endpoint:
                raise Exception("EC2 API endpoint not configured")
            
            # Execute query via HTTP API
            result_data = await self._execute_http_query(query.sql)
            
            execution_time = time.time() - start_time
            cost = (execution_time / 3600) * self.cost_per_hour
            
            return QueryResult(
                query_id=query.id,
                service_used=f"EC2-{self.instance_type}",
                result_data=result_data,
                execution_time=execution_time,
                cost=cost,
                status="SUCCESS"
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            cost = (execution_time / 3600) * self.cost_per_hour
            
            return QueryResult(
                query_id=query.id,
                service_used=f"EC2-{self.instance_type}",
                result_data={"error": str(e)},
                execution_time=execution_time,
                cost=cost,
                status="ERROR"
            )
    
    async def _execute_http_query(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query via HTTP API call to EC2 instance"""
        headers = {'Content-Type': 'application/json'}
        if self.api_key:
            headers['Authorization'] = f'Bearer {self.api_key}'
        
        payload = {
            'sql': sql,
            'format': 'json'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.api_endpoint}/query",
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=300)  # 5 minute timeout
            ) as response:
                
                if response.status != 200:
                    error_text = await response.text()
                    raise Exception(f"HTTP {response.status}: {error_text}")
                
                result = await response.json()
                
                return {
                    "rows": result.get("rows", 0),
                    "columns": result.get("columns", []),
                    "data": result.get("data", []),
                    "executed_on": f"EC2-{self.instance_id}",
                    "execution_time": result.get("execution_time", 0)
                }
    
    async def execute_query_simulation(self, query: Query) -> QueryResult:
        """Simulate EC2 executing SQL query"""
        execution_time = random.uniform(2, 8)  # 2-8 seconds
        await asyncio.sleep(execution_time)
        
        cost = (execution_time / 3600) * self.cost_per_hour
        
        return QueryResult(
            query_id=query.id,
            service_used=f"EC2-{self.instance_type}",
            result_data={"rows": random.randint(100, 1000), "columns": ["col1", "col2", "col3"]},
            execution_time=execution_time,
            cost=cost,
            status="SUCCESS"
        )
    
    def is_available(self) -> bool:
        if self.simulation_mode:
            return True
        
        # Check EC2 instance status
        if not self.ec2_client or not self.instance_id:
            return False
        
        try:
            response = self.ec2_client.describe_instances(InstanceIds=[self.instance_id])
            instance = response['Reservations'][0]['Instances'][0]
            return instance['State']['Name'] == 'running'
        except Exception:
            return False