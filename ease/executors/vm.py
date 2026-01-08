"""
VM (Virtual Machine) Executor

Example: AWS EC2, Azure VM, GCP Compute Engine

Supports:
- Single VM worker
- Multiple static workers (round-robin)
- Dynamic worker management via API
"""

import time
from typing import Dict, List
from .base import BaseExecutor
from ..core import Query, ExecutionResult, ExecutionStatus, ServiceConfig


class VMExecutor(BaseExecutor):
    """
    VM Executor

    Characteristics:
    - Fixed resources (CPU, memory, IO)
    - Persistent instance
    - Billing by time
    """

    def __init__(self, config: ServiceConfig):
        super().__init__(config)

        # VM-specific configuration
        self.cpu_cores = config.config.get('cpu_cores', 32)
        self.memory_gb = config.config.get('memory_gb', 128)
        self.io_bandwidth_mbps = config.config.get('io_bandwidth_mbps', 1250)
        self.cost_per_hour = config.config.get('cost_per_hour', 1.536)

        # Endpoint configuration
        endpoint_config = config.config.get('endpoint')
        if not endpoint_config:
            raise ValueError(f"VM executor '{self.name}' missing 'endpoint' configuration")

        # Support both single endpoint and list of endpoints
        if isinstance(endpoint_config, list):
            self.endpoints = endpoint_config.copy()
        else:
            self.endpoints = [endpoint_config]

        # Validate endpoints
        for ep in self.endpoints:
            if ep.startswith('<') or ep == '':
                raise ValueError(
                    f"VM executor '{self.name}' has placeholder endpoint '{ep}'. "
                    f"Please configure valid endpoint(s) (e.g., 'http://192.168.1.10:8081')"
                )

        # Round-robin counter for load balancing
        self._endpoint_index = 0

    def add_worker(self, endpoint: str) -> bool:
        """
        Add a new worker endpoint (for auto-scaling)

        Args:
            endpoint: Worker endpoint URL

        Returns:
            True if added, False if already exists
        """
        if endpoint not in self.endpoints:
            self.endpoints.append(endpoint)
            print(f"[{self.name}] Added worker: {endpoint} (total: {len(self.endpoints)})")
            return True
        return False

    def remove_worker(self, endpoint: str) -> bool:
        """
        Remove a worker endpoint (for auto-scaling)

        Args:
            endpoint: Worker endpoint URL

        Returns:
            True if removed, False if not found
        """
        if endpoint in self.endpoints:
            self.endpoints.remove(endpoint)
            print(f"[{self.name}] Removed worker: {endpoint} (remaining: {len(self.endpoints)})")
            return True
        return False

    def get_workers(self) -> List[str]:
        """Get list of current worker endpoints"""
        return self.endpoints.copy()

    def _get_next_endpoint(self) -> str:
        """Get next endpoint using round-robin load balancing"""
        if not self.endpoints:
            raise RuntimeError(f"VM executor '{self.name}' has no available workers")

        endpoint = self.endpoints[self._endpoint_index % len(self.endpoints)]
        self._endpoint_index = (self._endpoint_index + 1) % len(self.endpoints)
        return endpoint

    async def execute(self, query: Query) -> ExecutionResult:
        """
        Execute query on remote VM worker

        Sends the query to the VM worker via HTTP and waits for completion.

        Args:
            query: Query to execute

        Returns:
            ExecutionResult with timing and cost information
        """
        start_time = time.time()

        try:
            # Try to import aiohttp
            try:
                import aiohttp
            except ImportError:
                print(f"[{self.name}] Warning: aiohttp not installed, cannot execute query")
                return ExecutionResult(
                    query_id=query.query_id,
                    service_used=self.name,
                    status=ExecutionStatus.FAILED,
                    execution_time=0.0,
                    cost=0.0,
                    error="aiohttp not installed"
                )

            # Prepare request payload
            payload = {
                'query_id': query.query_id,
                'sql': query.sql,
                'metadata': query.metadata
            }

            # Send query to VM worker
            async with aiohttp.ClientSession() as session:
                url = f"{self._get_next_endpoint()}/execute"

                try:
                    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=3600)) as response:
                        if response.status == 200:
                            result_data = await response.json()

                            # Check execution status
                            if result_data.get('status') == 'success':
                                execution_time = result_data.get('execution_time', 0.0)

                                # Calculate cost based on execution time
                                cost = self.estimate_cost(query, execution_time)

                                # Record total time (includes network overhead)
                                total_time = time.time() - start_time

                                print(f"[{self.name}] Query {query.query_id} completed: "
                                      f"{execution_time:.3f}s execution, {total_time:.3f}s total, "
                                      f"${cost:.6f}")

                                return ExecutionResult(
                                    query_id=query.query_id,
                                    service_used=self.name,
                                    status=ExecutionStatus.SUCCESS,
                                    execution_time=execution_time,
                                    cost=cost,
                                    metadata={
                                        'total_time': total_time,
                                        'row_count': result_data.get('row_count'),
                                        'result_preview': result_data.get('result_preview')
                                    }
                                )
                            else:
                                # Query execution failed on VM
                                error_msg = result_data.get('error_message', 'Unknown error')
                                execution_time = result_data.get('execution_time', 0.0)
                                cost = self.estimate_cost(query, execution_time)

                                print(f"[{self.name}] Query {query.query_id} failed: {error_msg}")

                                return ExecutionResult(
                                    query_id=query.query_id,
                                    service_used=self.name,
                                    status=ExecutionStatus.FAILED,
                                    execution_time=execution_time,
                                    cost=cost,
                                    error=error_msg
                                )
                        else:
                            # HTTP error
                            error_text = await response.text()
                            print(f"[{self.name}] HTTP error {response.status}: {error_text}")

                            return ExecutionResult(
                                query_id=query.query_id,
                                service_used=self.name,
                                status=ExecutionStatus.FAILED,
                                execution_time=0.0,
                                cost=0.0,
                                error=f"HTTP {response.status}: {error_text}"
                            )

                except aiohttp.ClientError as e:
                    # Network error
                    print(f"[{self.name}] Network error: {e}")

                    return ExecutionResult(
                        query_id=query.query_id,
                        service_used=self.name,
                        status=ExecutionStatus.FAILED,
                        execution_time=0.0,
                        cost=0.0,
                        error=f"Network error: {str(e)}"
                    )

        except Exception as e:
            # Unexpected error
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
        VM cost model: time × hourly_rate

        Args:
            query: Query object
            estimated_time: Estimated execution time (seconds)

        Returns:
            Estimated cost in dollars
        """
        cost_per_second = self.cost_per_hour / 3600.0
        return estimated_time * cost_per_second

    def get_capacity(self) -> Dict[str, float]:
        """Get VM capacity"""
        return {
            'cpu_cores': self.cpu_cores,
            'memory_gb': self.memory_gb,
            'io_bandwidth_mbps': self.io_bandwidth_mbps
        }
