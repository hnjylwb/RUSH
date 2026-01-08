#!/usr/bin/env python3
"""
Submit queries to the scheduling server
"""

import asyncio
import argparse
from ease.client import SchedulingClient


async def main():
    parser = argparse.ArgumentParser(description="Query Scheduling Client")
    parser.add_argument('benchmark', choices=['tpch', 'ssb', 'clickbench'],
                       help='Benchmark to run')
    parser.add_argument('queries', nargs='*',
                       help='Query IDs to submit (e.g., q1 q2 q3). If not specified, list available queries.')
    parser.add_argument('--server', default='http://localhost:8080',
                       help='Server URL (default: http://localhost:8080)')
    parser.add_argument('--client-id', default=None,
                       help='Client identifier (auto-generated if not provided)')
    parser.add_argument('--resources', default=None,
                       help='CSV file with query resource requirements')

    args = parser.parse_args()

    print("=" * 80)
    print(f"Query Scheduling Client - {args.benchmark.upper()}")
    print("=" * 80)

    # Create client
    client = SchedulingClient(
        server_url=args.server,
        client_id=args.client_id,
        resource_file=args.resources
    )
    print(f"\nClient ID: {client.client_id}")
    print(f"Server: {args.server}")

    # Load all available queries
    print(f"\nLoading {args.benchmark} queries...")
    all_queries = client.load_queries_from_benchmark(args.benchmark)

    # Create a mapping of query names to query objects
    query_map = {}
    for query in all_queries:
        # Extract query name from query_id (e.g., "tpch_q1" -> "q1")
        query_name = query.query_id.split('_')[-1]
        query_map[query_name.lower()] = query

    # If no query IDs specified, list available queries and exit
    if not args.queries:
        print(f"\nAvailable queries in {args.benchmark}:")
        print("-" * 80)
        for query_name in sorted(query_map.keys()):
            print(f"  - {query_name}")
        print(f"\nUsage: python run_client.py {args.benchmark} q1 q2 q3 ...")
        return

    # Filter queries based on user input
    queries_to_submit = []
    for query_id in args.queries:
        query_id_lower = query_id.lower()
        if query_id_lower in query_map:
            queries_to_submit.append(query_map[query_id_lower])
        else:
            print(f"WARNING: Query '{query_id}' not found in {args.benchmark}")

    if not queries_to_submit:
        print("ERROR: No valid queries to submit")
        return

    print(f"\nSelected {len(queries_to_submit)} queries to submit")

    # Check server health
    print("\nChecking server health...")
    is_healthy = await client.health_check()
    if not is_healthy:
        print("ERROR: Server is not reachable or not running")
        print(f"Please start the server first: python run_server.py")
        return

    print("Server is running")

    # Submit queries
    print(f"\nSubmitting queries to server...")
    print("-" * 80)

    for i, query in enumerate(queries_to_submit, 1):
        try:
            task_id = await client.submit_query(query)
            print(f"[{i}/{len(queries_to_submit)}] {query.query_id} → Task ID: {task_id}")
        except Exception as e:
            print(f"[{i}/{len(queries_to_submit)}] {query.query_id} → ERROR: {e}")

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


if __name__ == "__main__":
    asyncio.run(main())
