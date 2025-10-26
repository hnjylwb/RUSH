import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.rush_core import RUSHCore

async def demo():
    rush = RUSHCore()
    
    print("=== RUSH Time-Sliced Resource Scheduling Demo ===\\n")
    
    # Start background schedulers
    print("Starting background schedulers...")
    await rush.start_background_schedulers()
    
    # Wait a moment for schedulers to start
    await asyncio.sleep(0.5)
    
    # Test queries
    test_queries = [
        "SELECT * FROM users WHERE age > 25",
        "SELECT COUNT(*) FROM orders GROUP BY region",
        "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        "SELECT AVG(salary) FROM employees GROUP BY department ORDER BY AVG(salary) DESC"
    ]
    
    # Submit queries (non-blocking)
    query_ids = []
    for i, sql in enumerate(test_queries, 1):
        print(f"\\n--- Submitting Test Query {i} ---")
        print(f"SQL: {sql}")
        print("-" * 50)
        
        query_id = await rush.submit_query(sql)
        query_ids.append(query_id)
        
        # Show queue status after each submission
        print(f"Queue status: {rush.get_queue_status()}")
        
        # Show resource utilization
        resource_util = rush.get_resource_utilization()
        for service, executors in resource_util.items():
            if executors:  # Only show if there's utilization
                for executor_name, utilization in executors.items():
                    if utilization:  # Only show if this executor has utilization
                        print(f"{service}.{executor_name} resource utilization: {[f'{u:.2f}' for u in utilization[:5]]}")
        
        print("=" * 60)
        
        # Small delay between submissions
        await asyncio.sleep(1)
    
    # Let background schedulers process queries
    print("\\nLetting background schedulers process queries...")
    await asyncio.sleep(10)
    
    # Show final status
    print(f"\\nFinal queue status: {rush.get_queue_status()}")
    
    # Show final resource utilization
    print("\\nFinal resource utilization:")
    resource_util = rush.get_resource_utilization()
    for service, executors in resource_util.items():
        if executors:
            for executor_name, utilization in executors.items():
                if utilization:
                    print(f"  {service}.{executor_name}: {[f'{u:.2f}' for u in utilization[:8]]}")
    
    # Stop background schedulers
    print("\\nStopping background schedulers...")
    rush.stop_background_schedulers()

if __name__ == "__main__":
    asyncio.run(demo())