"""
FaaS (Function as a Service) Executor

Example: AWS Lambda, Azure Functions, GCP Cloud Functions

Executes queries in parallel across multiple Lambda instances with data partitioning.
"""

import time
import json
import asyncio
from typing import Dict, List, Any, Optional
from .base import BaseExecutor
from ..core import Query, ExecutionResult, ExecutionStatus, ServiceConfig


class FaaSExecutor(BaseExecutor):
    """
    FaaS Executor

    Characteristics:
    - Serverless, auto-scaling
    - Billing by execution time × memory
    - Cluster of instances
    """

    def __init__(self, config: ServiceConfig):
        super().__init__(config)

        # Lambda pricing
        self.cost_per_gb_second = config.config['cost_per_gb_second']

        # Lambda memory sizes (in GB)
        memory_sizes = config.config['memory_sizes_gb']

        # Generate Lambda configurations from memory sizes
        self.lambda_configs = []
        for memory_gb in sorted(memory_sizes):
            self.lambda_configs.append({
                'name': f'lambda-{memory_gb}gb',
                'function_name': f'ease-query-executor-{memory_gb}gb',
                'memory_gb': memory_gb
            })

        if not self.lambda_configs:
            raise ValueError(
                f"FaaS executor '{self.name}' has no memory configurations. "
                f"Please specify 'memory_sizes_gb' in config"
            )

        print(f"[{self.name}] Initialized with {len(self.lambda_configs)} Lambda configurations:")
        for cfg in self.lambda_configs:
            print(f"  - {cfg['name']}: {cfg['memory_gb']}GB")

    def _select_lambda_instance(self, query: Query) -> Dict:
        """
        Select appropriate Lambda instance based on query requirements

        Args:
            query: Query to execute

        Returns:
            Lambda configuration dict with function_name, memory_gb, etc.
        """
        # Get memory requirement from query metadata
        # If not specified, use a default based on data size
        required_memory_gb = None
        if query.metadata:
            required_memory_gb = query.metadata.get('required_memory_gb')

        # If not specified, estimate based on data scanned
        if required_memory_gb is None:
            data_scanned_gb = query.metadata.get('data_scanned', 0) / (1024 ** 3) if query.metadata else 0
            # Simple heuristic: 2x data size for memory
            required_memory_gb = max(2, data_scanned_gb * 2)

        # Select smallest Lambda instance that meets requirement
        selected = None
        for cfg in self.lambda_configs:
            if cfg.get('memory_gb', 0) >= required_memory_gb:
                selected = cfg
                break

        # If no instance is large enough, use the largest one
        if selected is None:
            selected = self.lambda_configs[-1]

        print(f"[{self.name}] Query {query.query_id}: Selected {selected.get('name')} "
              f"({selected.get('memory_gb')}GB) for {required_memory_gb:.1f}GB requirement")

        return selected

    def _allocate_partitions(self, num_partitions: int, num_instances: int) -> List[List[int]]:
        """
        Allocate partitions to instances evenly

        Args:
            num_partitions: Total number of data partitions
            num_instances: Number of Lambda instances to use

        Returns:
            List of partition lists, one per instance
        """
        # Use minimum of available instances and partitions
        instances_to_use = min(num_instances, num_partitions)

        # Calculate base partitions per instance and remainder
        base_partitions = num_partitions // instances_to_use
        remainder = num_partitions % instances_to_use

        allocation = []
        partition_id = 0

        for i in range(instances_to_use):
            # First 'remainder' instances get one extra partition
            partitions_for_this = base_partitions + (1 if i < remainder else 0)
            instance_partitions = list(range(partition_id, partition_id + partitions_for_this))
            allocation.append(instance_partitions)
            partition_id += partitions_for_this

        return allocation

    async def _invoke_lambda(self, query: Query, partition_ids: List[int],
                            instance_id: int, lambda_config: Dict) -> Dict[str, Any]:
        """
        Invoke a single Lambda instance

        Args:
            query: Query to execute
            partition_ids: List of partition IDs for this instance
            instance_id: Instance identifier
            lambda_config: Lambda configuration with function_name

        Returns:
            Result from Lambda execution
        """
        try:
            import boto3
        except ImportError:
            return {
                'status': 'error',
                'error': 'boto3 not installed',
                'partition_ids': partition_ids,
                'instance_id': instance_id
            }

        payload = {
            'query_id': query.query_id,
            'sql': query.sql,
            'partition_ids': partition_ids,
            'instance_id': instance_id,
            'metadata': query.metadata
        }

        try:
            lambda_client = boto3.client('lambda')
            function_name = lambda_config.get('function_name')

            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )

            # Parse response
            response_payload = json.loads(response['Payload'].read())

            if response['StatusCode'] == 200:
                return response_payload
            else:
                return {
                    'status': 'error',
                    'error': f"Lambda returned status {response['StatusCode']}",
                    'partition_ids': partition_ids,
                    'instance_id': instance_id
                }

        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'partition_ids': partition_ids,
                'instance_id': instance_id
            }

    def _aggregate_results(self, lambda_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate results from multiple Lambda instances

        Args:
            lambda_results: List of results from each Lambda

        Returns:
            Aggregated result
        """
        total_rows = 0
        total_execution_time = 0.0
        failed_instances = []
        successful_instances = 0

        for result in lambda_results:
            if result.get('status') == 'success':
                total_rows += result.get('row_count', 0)
                exec_time = result.get('execution_time', 0.0)
                # Max execution time (parallel execution)
                total_execution_time = max(total_execution_time, exec_time)
                successful_instances += 1
            else:
                failed_instances.append({
                    'instance_id': result.get('instance_id'),
                    'partition_ids': result.get('partition_ids'),
                    'error': result.get('error')
                })

        if failed_instances:
            return {
                'status': 'partial' if successful_instances > 0 else 'error',
                'execution_time': total_execution_time,
                'row_count': total_rows,
                'successful_instances': successful_instances,
                'failed_instances': failed_instances
            }
        else:
            return {
                'status': 'success',
                'execution_time': total_execution_time,
                'row_count': total_rows,
                'successful_instances': successful_instances
            }

    def _decide_num_instances(self, query: Query, num_partitions: int, lambda_config: Dict) -> int:
        """
        Decide how many parallel Lambda invocations to use for this query

        This uses a cost model to determine optimal instance count.
        For now, we use a simple strategy based on data size and partitions.

        Args:
            query: Query to execute
            num_partitions: Number of data partitions
            lambda_config: Selected Lambda configuration

        Returns:
            Number of parallel invocations to use
        """
        # Get memory in GB
        memory_gb = lambda_config.get('memory_gb', 2)

        # Estimate how many instances we can effectively use
        # Larger memory instances can handle more partitions in parallel
        max_instances = min(100, memory_gb * 10)

        # Use minimum of max instances and partitions
        return min(max_instances, num_partitions)

    async def execute(self, query: Query) -> ExecutionResult:
        """
        Execute query on FaaS cluster with data partitioning

        Args:
            query: Query to execute

        Returns:
            ExecutionResult with aggregated results
        """
        start_time = time.time()

        try:
            # Select appropriate Lambda instance based on query requirements
            lambda_config = self._select_lambda_instance(query)

            # Get partition information from query metadata
            num_partitions = query.metadata.get('num_partitions', 100) if query.metadata else 100

            # Determine how many instances to use based on cost model
            instances_to_use = self._decide_num_instances(query, num_partitions, lambda_config)

            # Allocate partitions to instances
            partition_allocation = self._allocate_partitions(num_partitions, instances_to_use)

            print(f"[{self.name}] Query {query.query_id}: Using {instances_to_use} parallel invocations "
                  f"of {lambda_config.get('name')} for {num_partitions} partitions")

            # Invoke all Lambda instances in parallel
            tasks = []
            for instance_id, partition_ids in enumerate(partition_allocation):
                task = self._invoke_lambda(query, partition_ids, instance_id, lambda_config)
                tasks.append(task)

            # Wait for all instances to complete
            lambda_results = await asyncio.gather(*tasks)

            # Aggregate results
            aggregated = self._aggregate_results(lambda_results)

            # Calculate total time and cost
            total_time = time.time() - start_time
            execution_time = aggregated['execution_time']

            # Calculate cost: instances × execution_time × memory × rate
            memory_gb = lambda_config.get('memory_gb', 2)
            cost = instances_to_use * execution_time * memory_gb * self.cost_per_gb_second

            # Determine status
            if aggregated['status'] == 'success':
                status = ExecutionStatus.SUCCESS
                error = None
            elif aggregated['status'] == 'partial':
                status = ExecutionStatus.FAILED
                error = f"{len(aggregated['failed_instances'])} instances failed"
            else:
                status = ExecutionStatus.FAILED
                error = "All instances failed"

            print(f"[{self.name}] Query {query.query_id} completed: "
                  f"{execution_time:.3f}s execution, {total_time:.3f}s total, "
                  f"{aggregated['row_count']} rows, ${cost:.6f}")

            return ExecutionResult(
                query_id=query.query_id,
                service_used=self.name,
                status=status,
                execution_time=execution_time,
                cost=cost,
                error=error,
                metadata={
                    'total_time': total_time,
                    'row_count': aggregated['row_count'],
                    'instances_used': instances_to_use,
                    'successful_instances': aggregated['successful_instances'],
                    'num_partitions': num_partitions,
                    'failed_instances': aggregated.get('failed_instances', []),
                    'lambda_config': lambda_config.get('name'),
                    'memory_gb': memory_gb
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

    def estimate_cost(self, query: Query, estimated_time: float) -> float:
        """
        FaaS cost model: gb_seconds × rate

        Uses the appropriate Lambda instance based on query requirements.

        Args:
            query: Query object
            estimated_time: Estimated execution time (seconds)

        Returns:
            Estimated cost in dollars
        """
        # Select appropriate Lambda instance
        lambda_config = self._select_lambda_instance(query)
        memory_gb = lambda_config.get('memory_gb', 2)

        # Estimate number of parallel invocations
        num_partitions = query.metadata.get('num_partitions', 100) if query.metadata else 100
        instances_to_use = self._decide_num_instances(query, num_partitions, lambda_config)

        # Calculate cost
        gb_seconds = estimated_time * memory_gb * instances_to_use
        return gb_seconds * self.cost_per_gb_second

    def get_capacity(self) -> Dict[str, float]:
        """Get FaaS capacity information"""
        return {
            'num_lambda_configs': len(self.lambda_configs),
            'min_memory_gb': self.lambda_configs[0].get('memory_gb') if self.lambda_configs else 0,
            'max_memory_gb': self.lambda_configs[-1].get('memory_gb') if self.lambda_configs else 0,
            'cost_per_gb_second': self.cost_per_gb_second
        }
