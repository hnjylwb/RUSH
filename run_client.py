#!/usr/bin/env python3
"""
Query scheduling client - supports both interactive and one-time modes
"""

import asyncio
import argparse
from ease.client import SchedulingClient


class InteractiveCLI:
    """Interactive command-line interface"""

    def __init__(self, client: SchedulingClient):
        self.client = client
        self.running = True

    def print_banner(self):
        """Print welcome banner"""
        print("=" * 80)
        print("Query Scheduling Client - Interactive Mode")
        print("=" * 80)
        print(f"Client ID: {self.client.client_id}")
        print(f"Server: {self.client.server_url}")
        if self.client.resource_dir:
            print(f"Resource Directory: {self.client.resource_dir}")
        print("\nType 'help' for available commands")
        print("=" * 80)

    def print_help(self):
        """Print help message"""
        print("\nAvailable commands:")
        print("  list <benchmark>               - List available queries")
        print("  submit <benchmark> <q1> [q2]   - Submit queries")
        print("  status                         - Get server status")
        print("  tasks                          - Show submitted tasks count")
        print("  resources                      - Show cached resources")
        print("  clear                          - Clear resource cache")
        print("  help                           - Show this help")
        print("  exit / quit                    - Exit")
        print("\nExamples:")
        print("  list tpch")
        print("  submit tpch 1 2 3")

    async def cmd_list(self, args):
        """List available queries"""
        if len(args) < 1:
            print("Error: Usage: list <benchmark>")
            return

        benchmark = args[0]
        try:
            query_ids = self.client.list_available_queries(benchmark)
            print(f"\nAvailable queries in {benchmark}:")
            print("-" * 80)
            for query_id in query_ids:
                resource = self.client.load_resource(f"{benchmark}_{query_id}")
                marker = "✓" if resource else " "
                print(f"  [{marker}] {query_id}")
            print(f"\nTotal: {len(query_ids)} queries (✓ = has resource profile)")
        except Exception as e:
            print(f"Error: {e}")

    async def cmd_submit(self, args):
        """Submit queries"""
        if len(args) < 2:
            print("Error: Usage: submit <benchmark> <q1> [q2 ...]")
            return

        benchmark = args[0]
        query_ids = args[1:]

        try:
            is_healthy = await self.client.health_check()
            if not is_healthy:
                print("Error: Server is not reachable")
                return

            print(f"\nLoading {len(query_ids)} queries from {benchmark}...")
            queries = self.client.load_queries_from_benchmark(benchmark, query_ids=query_ids)

            if not queries:
                print("Error: No valid queries found")
                return

            print(f"Submitting {len(queries)} queries...")
            print("-" * 80)

            for i, query in enumerate(queries, 1):
                try:
                    resource = self.client.load_resource(query.query_id)
                    marker = "✓" if resource else " "
                    task_id = await self.client.submit_query(query)
                    print(f"[{i}/{len(queries)}] [{marker}] {query.query_id} → {task_id[:8]}...")
                except Exception as e:
                    print(f"[{i}/{len(queries)}] {query.query_id} → ERROR: {e}")

            print(f"\n{len(queries)} queries submitted")
        except Exception as e:
            print(f"Error: {e}")

    async def cmd_status(self, args):
        """Get server status"""
        try:
            status = await self.client.get_server_status()
            print("\nServer Status:")
            print("-" * 80)
            print(f"Running: {status.get('running', 'unknown')}")
            print(f"Total tasks: {status.get('total_tasks', 0)}")
            print(f"Queue sizes: {status.get('queues', {})}")
        except Exception as e:
            print(f"Error: {e}")

    async def cmd_tasks(self, args):
        """Show submitted tasks"""
        count = self.client.get_submitted_count()
        print(f"\nSubmitted tasks: {count}")

    async def cmd_resources(self, args):
        """Show cached resources"""
        count = len(self.client.resources)
        print(f"\nCached resource profiles: {count}")
        if count > 0:
            print("\nCached queries:")
            for query_id in sorted(self.client.resources.keys()):
                resource = self.client.resources[query_id]
                samples = len(resource.get('cpu', []))
                duration = samples * 0.1
                print(f"  - {query_id}: {samples} samples ({duration:.1f}s)")

    async def cmd_clear(self, args):
        """Clear resource cache"""
        count = len(self.client.resources)
        self.client.resources.clear()
        print(f"\nCleared {count} cached resource profiles")

    async def process_command(self, line):
        """Process a command line"""
        line = line.strip()
        if not line:
            return

        parts = line.split()
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd in ['exit', 'quit']:
            self.running = False
            print("\nGoodbye!")
        elif cmd == 'help':
            self.print_help()
        elif cmd == 'list':
            await self.cmd_list(args)
        elif cmd == 'submit':
            await self.cmd_submit(args)
        elif cmd == 'status':
            await self.cmd_status(args)
        elif cmd == 'tasks':
            await self.cmd_tasks(args)
        elif cmd == 'resources':
            await self.cmd_resources(args)
        elif cmd == 'clear':
            await self.cmd_clear(args)
        else:
            print(f"Unknown command: {cmd}")
            print("Type 'help' for available commands")

    async def run(self):
        """Run interactive mode"""
        self.print_banner()

        while self.running:
            try:
                loop = asyncio.get_event_loop()
                line = await loop.run_in_executor(None, input, "\n> ")
                await self.process_command(line)
            except KeyboardInterrupt:
                print("\n\nUse 'exit' or 'quit' to exit")
            except EOFError:
                self.running = False
                print("\nGoodbye!")
            except Exception as e:
                print(f"Error: {e}")


async def run_interactive_mode(client: SchedulingClient):
    """Run client in interactive mode"""
    cli = InteractiveCLI(client)
    await cli.run()


async def run_onetime_mode(client: SchedulingClient, benchmark: str, query_ids: list):
    """Run client in one-time submission mode"""
    print("=" * 80)
    print(f"Query Scheduling Client - {benchmark.upper()}")
    print("=" * 80)
    print(f"\nClient ID: {client.client_id}")
    print(f"Server: {client.server_url}")

    # If no query IDs specified, list available queries
    if not query_ids:
        print(f"\nAvailable queries in {benchmark}:")
        print("-" * 80)
        try:
            available = client.list_available_queries(benchmark)
            for query_id in available:
                print(f"  - {query_id}")
            print(f"\nUsage: python run_client.py {benchmark} q1 q2 q3 ...")
        except Exception as e:
            print(f"Error: {e}")
        return

    # Load and submit queries
    print(f"\nLoading specified queries from {benchmark}...")
    try:
        queries = client.load_queries_from_benchmark(benchmark, query_ids=query_ids)

        if not queries:
            print("ERROR: No valid queries found")
            return

        print(f"Loaded {len(queries)} queries")

        # Check server health
        print("\nChecking server health...")
        is_healthy = await client.health_check()
        if not is_healthy:
            print("ERROR: Server is not reachable or not running")
            print("Please start the server first: python run_server.py")
            return

        print("Server is running")

        # Submit queries
        print(f"\nSubmitting queries to server...")
        print("-" * 80)

        for i, query in enumerate(queries, 1):
            try:
                task_id = await client.submit_query(query)
                print(f"[{i}/{len(queries)}] {query.query_id} → Task ID: {task_id}")
            except Exception as e:
                print(f"[{i}/{len(queries)}] {query.query_id} → ERROR: {e}")

        print(f"\n{client.get_submitted_count()} queries submitted successfully")

        # Get server status
        print("\n" + "=" * 80)
        print("Server Status")
        print("=" * 80)
        try:
            status = await client.get_server_status()
            print(f"Total tasks: {status['total_tasks']}")
            print(f"Queue sizes: {status['queues']}")
        except Exception as e:
            print(f"Could not get server status: {e}")

    except Exception as e:
        print(f"Error: {e}")


async def main():
    parser = argparse.ArgumentParser(description="Query Scheduling Client")
    parser.add_argument('benchmark', nargs='?',
                       choices=['tpch', 'ssb', 'clickbench'],
                       help='Benchmark to run (omit for interactive mode)')
    parser.add_argument('queries', nargs='*',
                       help='Query IDs to submit')
    parser.add_argument('--server', default='http://localhost:8080',
                       help='Server URL (default: http://localhost:8080)')
    parser.add_argument('--client-id', default=None,
                       help='Client identifier')
    parser.add_argument('--resources', default=None,
                       help='Directory containing query resource JSON files')
    parser.add_argument('-i', '--interactive', action='store_true',
                       help='Start in interactive mode')

    args = parser.parse_args()

    # Create client
    client = SchedulingClient(
        server_url=args.server,
        client_id=args.client_id,
        resource_dir=args.resources
    )

    # Determine mode
    if args.interactive or not args.benchmark:
        # Interactive mode
        await run_interactive_mode(client)
    else:
        # One-time mode
        await run_onetime_mode(client, args.benchmark, args.queries)


if __name__ == "__main__":
    asyncio.run(main())
