"""
VM Worker - Query execution service running on VM nodes

This service runs on each VM executor node and handles query execution using DuckDB.
It exposes an HTTP API for receiving queries and returning results.
"""

import asyncio
import time
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class QueryRequest:
    """Query execution request"""
    query_id: str
    sql: str
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class QueryResponse:
    """Query execution response"""
    query_id: str
    status: str  # "success" or "error"
    execution_time: float  # seconds
    row_count: Optional[int] = None
    error_message: Optional[str] = None
    result_preview: Optional[str] = None  # First few rows as string


class VMWorker:
    """
    VM Worker Service

    Runs on VM nodes to execute queries using DuckDB.
    Provides HTTP API for query submission and execution.
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8081,
                 data_dir: Optional[str] = None):
        """
        Initialize VM worker

        Args:
            host: Host address to bind to
            port: Port to listen on
            data_dir: Directory containing data files (for DuckDB)
        """
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.running = False
        self.app = None
        self.runner = None

        # Initialize DuckDB connection
        self.db_conn = None
        self._init_duckdb()

    def _init_duckdb(self):
        """Initialize DuckDB connection"""
        try:
            import duckdb
            self.db_conn = duckdb.connect(':memory:')

            # Configure DuckDB for optimal OLAP performance
            self.db_conn.execute("SET threads TO 32")
            self.db_conn.execute("SET memory_limit = '100GB'")

            # If data directory is specified, set up file paths
            if self.data_dir:
                self.db_conn.execute(f"SET home_directory = '{self.data_dir}'")

            print(f"DuckDB initialized successfully")
        except ImportError:
            print("Warning: duckdb not installed, worker will not be able to execute queries")
            self.db_conn = None

    def _setup_routes(self):
        """Setup HTTP API routes"""
        try:
            from aiohttp import web

            app = web.Application()

            # Health check
            async def health(request):
                return web.json_response({
                    "status": "ok",
                    "running": self.running,
                    "has_duckdb": self.db_conn is not None
                })

            # Execute query
            async def execute_query(request):
                try:
                    data = await request.json()

                    # Parse request
                    query_req = QueryRequest(
                        query_id=data['query_id'],
                        sql=data['sql'],
                        metadata=data.get('metadata')
                    )

                    # Execute query
                    response = await self.execute_query(query_req)

                    return web.json_response(asdict(response))

                except Exception as e:
                    return web.json_response({
                        "status": "error",
                        "error": str(e)
                    }, status=400)

            # Get worker status
            async def get_status(request):
                status = self.get_status()
                return web.json_response(status)

            app.router.add_get('/health', health)
            app.router.add_post('/execute', execute_query)
            app.router.add_get('/status', get_status)

            self.app = app
            return True

        except ImportError:
            print("Warning: aiohttp not installed, HTTP API disabled")
            return False

    async def execute_query(self, request: QueryRequest) -> QueryResponse:
        """
        Execute a query using DuckDB

        Args:
            request: Query request

        Returns:
            Query response with results and timing
        """
        if self.db_conn is None:
            return QueryResponse(
                query_id=request.query_id,
                status="error",
                execution_time=0.0,
                error_message="DuckDB not available"
            )

        try:
            # Record start time
            start_time = time.time()

            # Execute query
            result = self.db_conn.execute(request.sql)

            # Fetch results (to ensure query completes)
            rows = result.fetchall()
            row_count = len(rows)

            # Calculate execution time
            execution_time = time.time() - start_time

            # Get result preview (first 3 rows)
            preview = None
            if row_count > 0:
                preview_rows = rows[:3]
                preview = "\n".join([str(row) for row in preview_rows])
                if row_count > 3:
                    preview += f"\n... ({row_count - 3} more rows)"

            print(f"[VM Worker] Executed {request.query_id} in {execution_time:.3f}s "
                  f"({row_count} rows)")

            return QueryResponse(
                query_id=request.query_id,
                status="success",
                execution_time=execution_time,
                row_count=row_count,
                result_preview=preview
            )

        except Exception as e:
            execution_time = time.time() - start_time
            print(f"[VM Worker] Error executing {request.query_id}: {e}")

            return QueryResponse(
                query_id=request.query_id,
                status="error",
                execution_time=execution_time,
                error_message=str(e)
            )

    async def start(self):
        """Start the VM worker service"""
        if self.running:
            print("VM Worker is already running")
            return

        self.running = True
        print("=" * 80)
        print("VM Worker Started")
        print("=" * 80)
        print(f"DuckDB: {'Available' if self.db_conn else 'Not available'}")
        print(f"Data directory: {self.data_dir or 'Not specified'}")

        # Setup HTTP API
        has_http = self._setup_routes()
        if has_http:
            print(f"\nHTTP API: http://{self.host}:{self.port}")
            print("  - GET  /health   - Health check")
            print("  - POST /execute  - Execute query")
            print("  - GET  /status   - Get worker status")
        print()

        # Start HTTP server
        if has_http:
            from aiohttp import web
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            site = web.TCPSite(self.runner, self.host, self.port)
            await site.start()

            print(f"VM Worker is running on http://{self.host}:{self.port}")
            print("Press Ctrl+C to stop\n")

            # Keep running
            try:
                while self.running:
                    await asyncio.sleep(1)
            except asyncio.CancelledError:
                pass
        else:
            print("VM Worker could not start (missing dependencies)")

    async def stop(self):
        """Stop the VM worker service"""
        self.running = False

        # Close DuckDB connection
        if self.db_conn:
            self.db_conn.close()

        # Stop HTTP server
        if self.runner:
            await self.runner.cleanup()

        print("VM Worker stopped")

    def get_status(self) -> Dict:
        """Get worker status"""
        return {
            'running': self.running,
            'has_duckdb': self.db_conn is not None,
            'data_dir': self.data_dir
        }
