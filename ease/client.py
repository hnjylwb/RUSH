"""
Query scheduling client with HTTP API support
"""

from pathlib import Path
from typing import List, Optional, Dict
import uuid
import json

from .core import Query


class SchedulingClient:
    """
    Client for submitting queries to the scheduling server via HTTP API

    Can load queries from files and submit them to the server
    """

    def __init__(self, server_url: str = "http://localhost:8080",
                 client_id: Optional[str] = None,
                 resource_dir: Optional[str] = None):
        """
        Initialize client

        Args:
            server_url: Server URL (e.g., http://localhost:8080)
            client_id: Optional client identifier (auto-generated if not provided)
            resource_dir: Optional directory containing query resource JSON files
        """
        self.server_url = server_url.rstrip('/')
        self.client_id = client_id or str(uuid.uuid4())[:8]
        self.submitted_tasks: List[str] = []
        self.resource_dir = resource_dir
        self.resources: Dict[str, Dict] = {}  # Cache loaded resources

    def load_queries_from_benchmark(self, benchmark: str, query_ids: Optional[List[str]] = None) -> List[Query]:
        """
        Load queries from benchmark directory

        Args:
            benchmark: Benchmark name (tpch, ssb, clickbench)
            query_ids: Optional list of specific query IDs to load (e.g., ['1', '2', '3'])
                      If None, load all queries

        Returns:
            List of Query objects
        """
        query_dir = Path("queries") / benchmark
        if not query_dir.exists():
            raise ValueError(f"Benchmark '{benchmark}' not found in queries/")

        queries = []

        if query_ids:
            # Load only specified queries
            for query_id in query_ids:
                sql_file = query_dir / f"{query_id}.sql"
                if not sql_file.exists():
                    print(f"Warning: Query file not found: {sql_file}")
                    continue

                with open(sql_file, 'r') as f:
                    sql = f.read()

                full_query_id = f"{benchmark}_{query_id}"
                queries.append(Query(
                    query_id=full_query_id,
                    sql=sql
                ))
        else:
            # Load all queries (for listing purposes)
            for sql_file in sorted(query_dir.glob("*.sql")):
                with open(sql_file, 'r') as f:
                    sql = f.read()

                query_id = f"{benchmark}_{sql_file.stem}"
                queries.append(Query(
                    query_id=query_id,
                    sql=sql
                ))

        return queries

    def list_available_queries(self, benchmark: str) -> List[str]:
        """
        List available query IDs in a benchmark without loading SQL content

        Args:
            benchmark: Benchmark name (tpch, ssb, clickbench)

        Returns:
            List of query IDs
        """
        query_dir = Path("queries") / benchmark
        if not query_dir.exists():
            raise ValueError(f"Benchmark '{benchmark}' not found in queries/")

        query_ids = []
        for sql_file in sorted(query_dir.glob("*.sql")):
            query_ids.append(sql_file.stem)

        return query_ids

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
            sql=sql
        )]

    def load_resource(self, query_id: str) -> Optional[Dict]:
        """
        Load resource profile for a specific query

        Args:
            query_id: Query identifier (e.g., "tpch_1")

        Returns:
            Resource profile dict with keys: cpu, mem, net (each is a list of floats 0-1)
            Returns None if resource file not found
        """
        # Check cache first
        if query_id in self.resources:
            return self.resources[query_id]

        # If no resource directory specified, return None
        if not self.resource_dir:
            return None

        # Load from file
        resource_file = Path(self.resource_dir) / f"{query_id}.json"
        if not resource_file.exists():
            return None

        try:
            with open(resource_file, 'r') as f:
                resource_data = json.load(f)

            # Validate format
            if not all(key in resource_data for key in ['cpu', 'mem', 'net']):
                print(f"Warning: Invalid resource format for {query_id}")
                return None

            # Cache and return
            self.resources[query_id] = resource_data
            return resource_data

        except Exception as e:
            print(f"Warning: Failed to load resource file for {query_id}: {e}")
            return None

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
            'client_id': self.client_id
        }

        # Load and add resource profile if available
        resource_data = self.load_resource(query.query_id)
        if resource_data:
            payload['resource_profile'] = resource_data

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
