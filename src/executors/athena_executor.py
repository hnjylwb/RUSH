import asyncio
import random
import time
import boto3
from typing import Dict, Any
from .base_executor import BaseServiceExecutor
from ..models.query import Query
from ..models.result import QueryResult

class AthenaExecutor(BaseServiceExecutor):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.cost_per_tb = config.get('cost_per_tb', 5.0)
        
        # Real execution configuration
        if not self.simulation_mode:
            self.s3_bucket = config.get('s3_bucket', 'rush-athena-results')
            self.s3_prefix = config.get('s3_prefix', 'query-results/')
            self.database_name = config.get('database_name', 'rush_database')
            self.catalog_name = config.get('catalog_name', 'AwsDataCatalog')
            self.region_name = config.get('region_name', 'us-east-1')
            self.workgroup = config.get('workgroup', 'primary')
            
            # Initialize AWS clients
            self.athena_client = self._initialize_athena_client()
            self.s3_client = self._initialize_s3_client()
        
    def _initialize_athena_client(self):
        """Initialize AWS Athena client"""
        try:
            # Use default AWS credentials from ~/.aws/credentials or IAM role
            return boto3.client('athena', region_name=self.region_name)
        except Exception as e:
            print(f"[Athena-{self.name}] Error initializing Athena client: {e}")
            return None
    
    def _initialize_s3_client(self):
        """Initialize AWS S3 client for result retrieval"""
        try:
            return boto3.client('s3', region_name=self.region_name)
        except Exception as e:
            print(f"[Athena-{self.name}] Error initializing S3 client: {e}")
            return None
    
    async def execute_query_real(self, query: Query) -> QueryResult:
        """Execute query using AWS Athena"""
        start_time = time.time()
        
        try:
            if not self.athena_client or not self.s3_client:
                raise Exception("Athena or S3 client not configured")
            
            # Execute Athena query
            result_data = await self._execute_athena_query(query.sql)
            
            execution_time = time.time() - start_time
            
            # Get data scanned from query statistics
            data_scanned_gb = result_data.get('data_scanned_gb', 0.1)
            cost = (data_scanned_gb / 1024) * self.cost_per_tb
            
            return QueryResult(
                query_id=query.id,
                service_used="Athena",
                result_data=result_data,
                execution_time=execution_time,
                cost=cost,
                status="SUCCESS"
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            estimated_data_gb = 0.1
            cost = (estimated_data_gb / 1024) * self.cost_per_tb
            
            return QueryResult(
                query_id=query.id,
                service_used="Athena",
                result_data={"error": str(e)},
                execution_time=execution_time,
                cost=cost,
                status="ERROR"
            )
    
    async def _execute_athena_query(self, sql: str) -> Dict[str, Any]:
        """Execute query using AWS Athena service"""
        try:
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={
                    'Database': self.database_name,
                    'Catalog': self.catalog_name
                },
                ResultConfiguration={
                    'OutputLocation': f's3://{self.s3_bucket}/{self.s3_prefix}'
                },
                WorkGroup=self.workgroup
            )
            
            execution_id = response['QueryExecutionId']
            
            # Wait for query completion
            query_status = await self._wait_for_query_completion(execution_id)
            
            if query_status['QueryExecution']['Status']['State'] != 'SUCCEEDED':
                error = query_status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                raise Exception(f"Athena query failed: {error}")
            
            # Get query statistics
            statistics = query_status['QueryExecution'].get('Statistics', {})
            data_scanned_bytes = statistics.get('DataScannedInBytes', 0)
            data_scanned_gb = data_scanned_bytes / (1024 ** 3) if data_scanned_bytes else 0.1
            
            # Get query results
            results = await self._get_query_results(execution_id)
            
            return {
                "rows": len(results) - 1 if results else 0,  # Subtract header row
                "columns": results[0] if results else [],  # First row contains column names
                "data": results[1:] if len(results) > 1 else [],  # Data rows
                "execution_id": execution_id,
                "data_scanned_gb": data_scanned_gb,
                "engine_execution_time": statistics.get('EngineExecutionTimeInMillis', 0) / 1000,
                "queue_time": statistics.get('QueryQueueTimeInMillis', 0) / 1000,
                "planning_time": statistics.get('QueryPlanningTimeInMillis', 0) / 1000
            }
            
        except Exception as e:
            raise Exception(f"Athena query execution error: {str(e)}")
    
    async def _wait_for_query_completion(self, execution_id: str, max_wait_time: int = 300) -> Dict[str, Any]:
        """Wait for Athena query to complete"""
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            response = self.athena_client.get_query_execution(QueryExecutionId=execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return response
            
            # Wait before checking again
            await asyncio.sleep(2)
        
        raise Exception(f"Query {execution_id} timed out after {max_wait_time} seconds")
    
    async def _get_query_results(self, execution_id: str) -> list:
        """Get query results from Athena"""
        try:
            results = []
            next_token = None
            
            # Paginate through results
            while True:
                if next_token:
                    response = self.athena_client.get_query_results(
                        QueryExecutionId=execution_id,
                        NextToken=next_token,
                        MaxResults=1000  # Max allowed by Athena
                    )
                else:
                    response = self.athena_client.get_query_results(
                        QueryExecutionId=execution_id,
                        MaxResults=1000
                    )
                
                # Extract data from result set
                for row in response['ResultSet']['Rows']:
                    row_data = []
                    for column in row['Data']:
                        row_data.append(column.get('VarCharValue', ''))
                    results.append(row_data)
                
                # Check if there are more results
                next_token = response.get('NextToken')
                if not next_token:
                    break
            
            return results
            
        except Exception as e:
            raise Exception(f"Failed to retrieve query results: {str(e)}")
    
    async def execute_query_simulation(self, query: Query) -> QueryResult:
        """Simulate Athena executing analytics query"""
        execution_time = random.uniform(3, 12)  # 3-12 seconds
        await asyncio.sleep(execution_time)
        
        data_scanned_gb = random.uniform(0.1, 2.0)
        cost = (data_scanned_gb / 1024) * self.cost_per_tb
        
        return QueryResult(
            query_id=query.id,
            service_used="Athena",
            result_data={"rows": random.randint(1000, 10000), "data_scanned_gb": data_scanned_gb},
            execution_time=execution_time,
            cost=cost,
            status="SUCCESS"
        )
    
    def is_available(self) -> bool:
        if self.simulation_mode:
            return True
        
        # Check if Athena and S3 clients are available
        if not self.athena_client or not self.s3_client:
            return False
        
        try:
            # Test Athena service availability by listing workgroups
            self.athena_client.list_work_groups(MaxResults=1)
            return True
        except Exception:
            return False