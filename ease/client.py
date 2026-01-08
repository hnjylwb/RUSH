"""
Query scheduling client with HTTP API support
"""

from pathlib import Path
from typing import List, Optional, Dict
import uuid
import csv

from .core import Query, QueryType


class SchedulingClient:
    """
    Client for submitting queries to the scheduling server via HTTP API

    Can load queries from files and submit them to the server
    """

    def __init__(self, server_url: str = "http://localhost:8080",
                 client_id: Optional[str] = None,
                 resource_file: Optional[str] = None):
        """
        Initialize client

        Args:
            server_url: Server URL (e.g., http://localhost:8080)
            client_id: Optional client identifier (auto-generated if not provided)
            resource_file: Optional CSV file with query resource requirements
        """
        self.server_url = server_url.rstrip('/')
        self.client_id = client_id or str(uuid.uuid4())[:8]
        self.submitted_tasks: List[str] = []
        self.resources: Dict[str, Dict[str, float]] = {}

        # Load resource file if provided
        if resource_file:
            self.load_resources(resource_file)

    def load_queries_from_benchmark(self, benchmark: str) -> List[Query]:
        """
        Load queries from benchmark directory

        Args:
            benchmark: Benchmark name (tpch, ssb, clickbench)

        Returns:
            List of Query objects
        """
        query_dir = Path("queries") / benchmark
        if not query_dir.exists():
            raise ValueError(f"Benchmark '{benchmark}' not found in queries/")

        queries = []
        for sql_file in sorted(query_dir.glob("*.sql")):
            with open(sql_file, 'r') as f:
                sql = f.read()

            query_id = f"{benchmark}_{sql_file.stem}"
            queries.append(Query(
                query_id=query_id,
                sql=sql,
                query_type=QueryType.OLAP
            ))

        return queries

    def load_queries_from_file(self, file_path: str) -> List[Query]:
        """
        Load queries from a single SQL file

        Args:
            file_path: Path to SQL file

        Returns:
            List of Query objects (one query per file)
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Query file not found: {file_path}")

        with open(path, 'r') as f:
            sql = f.read()

        query_id = path.stem
        return [Query(
            query_id=query_id,
            sql=sql,
            query_type=QueryType.OLAP
        )]

    def load_resources(self, resource_file: str):
        """
        Load query resource requirements from CSV file

        Args:
            resource_file: Path to CSV file with columns: query_id, cpu_time, data_scanned, scale_factor
        """
        path = Path(resource_file)
        if not path.exists():
            print(f"Warning: Resource file not found: {resource_file}")
            return

        with open(path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                query_id = row['query_id']
                self.resources[query_id] = {
                    'cpu_time': float(row.get('cpu_time', 0)),
                    'data_scanned': float(row.get('data_scanned', 0)),
                    'scale_factor': float(row.get('scale_factor', 1.0))
                }

        print(f"Loaded resource requirements for {len(self.resources)} queries")

    async def submit_query(self, query: Query) -> str:
        """
        Submit a single query to the server

        Args:
            query: Query to submit

        Returns:
            Task ID
        """
        import aiohttp

        url = f"{self.server_url}/submit"
        payload = {
            'query_id': query.query_id,
            'sql': query.sql,
            'query_type': query.query_type.value,
            'client_id': self.client_id
        }

        # Add resource requirements if available
        if query.query_id in self.resources:
            payload['resource_requirements'] = self.resources[query.query_id]

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    task_id = data['task_id']
                    self.submitted_tasks.append(task_id)
                    return task_id
                else:
                    error = await response.text()
                    raise RuntimeError(f"Failed to submit query: {error}")

    async def submit_queries(self, queries: List[Query]) -> List[str]:
        """
        Submit multiple queries to the server

        Args:
            queries: List of queries to submit

        Returns:
            List of task IDs
        """
        task_ids = []
        for query in queries:
            task_id = await self.submit_query(query)
            task_ids.append(task_id)

        return task_ids

    async def get_task_status(self, task_id: str) -> dict:
        """
        Get status of a submitted task

        Args:
            task_id: Task ID

        Returns:
            Task status dictionary
        """
        import aiohttp

        url = f"{self.server_url}/task/{task_id}"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error = await response.text()
                    raise RuntimeError(f"Failed to get task status: {error}")

    async def get_server_status(self) -> dict:
        """
        Get server status

        Returns:
            Server status dictionary
        """
        import aiohttp

        url = f"{self.server_url}/status"

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error = await response.text()
                    raise RuntimeError(f"Failed to get server status: {error}")

    async def health_check(self) -> bool:
        """
        Check if server is running

        Returns:
            True if server is healthy, False otherwise
        """
        try:
            import aiohttp

            url = f"{self.server_url}/health"

            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=2)) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('running', False)
                    return False
        except Exception:
            return False

    def get_submitted_count(self) -> int:
        """Get number of submitted queries"""
        return len(self.submitted_tasks)
