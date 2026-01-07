"""
Demo script - 演示系统基本功能
"""

import asyncio
from ease import System, Query, QueryType


async def main():
    print("=" * 60)
    print("System Demo")
    print("=" * 60)

    # Initialize system
    print("\nInitializing system...")
    system = System(
        config_dir="config",
        resource_csv="data/query_resources.csv"
    )

    # Create test queries
    queries = [
        Query(
            query_id="Q1",
            sql="SELECT * FROM users WHERE age > 25",
            query_type=QueryType.OLTP
        ),
        Query(
            query_id="Q2",
            sql="SELECT category, COUNT(*) FROM products GROUP BY category",
            query_type=QueryType.OLAP
        ),
        Query(
            query_id="Q3",
            sql="SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
            query_type=QueryType.OLAP
        )
    ]

    # Submit queries
    print("\nSubmitting queries...")
    print("-" * 60)
    for query in queries:
        await system.submit_query(query)
        print()

    # Show system status
    print("\nSystem status:")
    print("-" * 60)
    status = system.get_status()
    print(f"Queue sizes: {status['queues']}")
    print(f"Executors: {status['executors']}")

    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
