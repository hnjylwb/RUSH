"""
QaaS (Query as a Service) Executor

Example: AWS Athena, BigQuery, Snowflake
"""

import time
import asyncio
from typing import Dict
from .base import BaseExecutor
from ..core import Query, ExecutionResult, ExecutionStatus, ServiceConfig


class QaaSExecutor(BaseExecutor):
    """
    QaaS Executor

    Characteristics:
    - Fully managed query service
    - Billing by data scanned
    - No resource management needed
    """

    def __init__(self, config: ServiceConfig):
        super().__init__(config)

        # QaaS-specific configuration
        self.cost_per_tb = config.config.get('cost_per_tb', 5.0)
        self.max_concurrent_queries = config.config.get('max_concurrent_queries', 30)
        self.scan_speed_tb_per_sec = config.config.get('scan_speed_tb_per_sec', 0.5)
        self.base_latency = config.config.get('base_latency', 5.0)

        # AWS Athena configuration (optional)
        self.database = config.config.get('database', 'default')
        self.output_location = config.config.get('output_location', 's3://aws-athena-query-results/')
        self.region = config.config.get('region', 'us-east-1')

        # Initialize boto3 client if available
        self.athena_client = None
        try:
            import boto3
            self.athena_client = boto3.client('athena', region_name=self.region)
        except ImportError:
            print(f"[{self.name}] Warning: boto3 not installed, cannot connect to AWS Athena")

    async def execute(self, query: Query) -> ExecutionResult:
        """
        Execute query on AWS Athena

        Args:
            query: Query to execute

        Returns:
            ExecutionResult with query statistics
        """
        start_time = time.time()

        try:
            if not self.athena_client:
                return ExecutionResult(
                    query_id=query.query_id,
                    service_used=self.name,
                    status=ExecutionStatus.FAILED,
                    execution_time=0.0,
                    cost=0.0,
                    error="AWS Athena client not initialized (boto3 not installed)"
                )

            # Start query execution
            print(f"[{self.name}] Query {query.query_id}: Submitting to Athena")

            response = self.athena_client.start_query_execution(
                QueryString=query.sql,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.output_location}
            )

            query_execution_id = response['QueryExecutionId']
            print(f"[{self.name}] Query {query.query_id}: Athena execution ID {query_execution_id}")

            # Poll for query completion
            execution_time, data_scanned_bytes, status, error = await self._wait_for_query_completion(
                query_execution_id
            )

            # Calculate cost based on data scanned
            data_scanned_tb = data_scanned_bytes / (1024 ** 4)
            cost = data_scanned_tb * self.cost_per_tb

            total_time = time.time() - start_time

            if status == 'SUCCEEDED':
                print(f"[{self.name}] Query {query.query_id} completed: "
                      f"{execution_time:.3f}s execution, {total_time:.3f}s total, "
                      f"{data_scanned_tb:.6f} TB scanned, ${cost:.6f}")

                return ExecutionResult(
                    query_id=query.query_id,
                    service_used=self.name,
                    status=ExecutionStatus.SUCCESS,
                    execution_time=execution_time,
                    cost=cost,
                    metadata={
                        'total_time': total_time,
                        'data_scanned_bytes': data_scanned_bytes,
                        'data_scanned_tb': data_scanned_tb,
                        'athena_execution_id': query_execution_id
                    }
                )
            else:
                print(f"[{self.name}] Query {query.query_id} failed: {error}")
                return ExecutionResult(
                    query_id=query.query_id,
                    service_used=self.name,
                    status=ExecutionStatus.FAILED,
                    execution_time=execution_time,
                    cost=cost,
                    error=error,
                    metadata={
                        'total_time': total_time,
                        'data_scanned_bytes': data_scanned_bytes,
                        'athena_execution_id': query_execution_id
                    }
                )

        except Exception as e:
            execution_time = time.time() - start_time
            print(f"[{self.name}] Unexpected error: {e}")

            return ExecutionResult(
                query_id=query.query_id,
                service_used=self.name,
                status=ExecutionStatus.FAILED,
                execution_time=execution_time,
                cost=0.0,
                error=f"Unexpected error: {str(e)}"
            )

    async def _wait_for_query_completion(self, query_execution_id: str) -> tuple:
        """
        Poll Athena until query completes

        Args:
            query_execution_id: Athena query execution ID

        Returns:
            Tuple of (execution_time, data_scanned_bytes, status, error_message)
        """
        poll_interval = 0.5  # Start with 0.5 second polling

        while True:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )

            status = response['QueryExecution']['Status']['State']

            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                # Extract execution statistics
                stats = response['QueryExecution']['Statistics']
                execution_time = stats.get('EngineExecutionTimeInMillis', 0) / 1000.0
                data_scanned_bytes = stats.get('DataScannedInBytes', 0)

                error_message = None
                if status != 'SUCCEEDED':
                    state_change = response['QueryExecution']['Status'].get('StateChangeReason', '')
                    error_message = f"Query {status}: {state_change}"

                return execution_time, data_scanned_bytes, status, error_message

            # Still running, wait before polling again
            await asyncio.sleep(poll_interval)

            # Gradually increase poll interval up to 2 seconds
            poll_interval = min(poll_interval * 1.5, 2.0)

    def estimate_cost(self, query: Query, estimated_time: float) -> float:
        """
        QaaS cost model: data_scanned_tb × rate

        Args:
            query: Query object
            estimated_time: Not used for QaaS (cost depends on data scanned)

        Returns:
            Estimated cost in dollars
        """
        # Get data scanned from query resource requirements
        if query.resource_requirements:
            data_scanned_bytes = query.resource_requirements.get('data_scanned', 0)
            data_scanned_tb = data_scanned_bytes / (1024 ** 4)
            return data_scanned_tb * self.cost_per_tb
        return 0.0

    def get_capacity(self) -> Dict[str, float]:
        """Get QaaS capacity"""
        return {
            'max_concurrent_queries': self.max_concurrent_queries,
            'scan_speed_tb_per_sec': self.scan_speed_tb_per_sec
        }
