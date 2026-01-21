"""
Lambda Function Handler for Query Execution

This is the Lambda function that runs on AWS Lambda (or similar FaaS platform).
Each Lambda instance executes SQL that has been pre-rewritten by the FaaS executor.
"""

import json
import time
from typing import Dict, Any


def execute_query(sql: str) -> Dict[str, Any]:
    """
    Execute SQL query using DuckDB

    Args:
        sql: Pre-rewritten SQL query (already contains partition references)

    Returns:
        Dict with execution results
    """
    try:
        import duckdb

        # Create DuckDB connection
        conn = duckdb.connect(':memory:')

        # Execute the pre-rewritten SQL
        start_time = time.time()
        result = conn.execute(sql)
        rows = result.fetchall()
        execution_time = time.time() - start_time

        conn.close()

        return {
            'status': 'success',
            'execution_time': execution_time,
            'row_count': len(rows),
            'result_preview': str(rows[:3]) if rows else None
        }

    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function

    Event format:
    {
        "query_id": "q1",
        "sql": "SELECT * FROM read_parquet(['s3://bucket/partition_0.parquet', ...]) AS table WHERE ...",
        "partition_ids": [0, 1, 2],
        "instance_id": 0,
        "metadata": { ... }
    }

    Note: SQL is already rewritten by FaaS executor to reference specific partitions.

    Args:
        event: Lambda event containing pre-rewritten SQL
        context: Lambda context

    Returns:
        Execution result
    """
    try:
        query_id = event.get('query_id')
        sql = event.get('sql')  # Already rewritten SQL
        partition_ids = event.get('partition_ids', [])
        instance_id = event.get('instance_id', 0)

        print(f"[Lambda {instance_id}] Query {query_id}: Processing partitions {partition_ids}")

        # Execute the pre-rewritten SQL directly
        result = execute_query(sql)

        # Add instance and partition info
        result['instance_id'] = instance_id
        result['query_id'] = query_id
        result['partition_ids'] = partition_ids

        print(f"[Lambda {instance_id}] Query {query_id}: {result['status']} - "
              f"{result.get('execution_time', 0):.3f}s")

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'status': 'error',
                'error': str(e),
                'instance_id': event.get('instance_id', 0),
                'partition_ids': event.get('partition_ids', [])
            })
        }


# For testing locally
if __name__ == "__main__":
    # Test event with pre-rewritten SQL
    test_event = {
        'query_id': 'test_q1',
        'sql': "SELECT COUNT(*) FROM read_parquet(['s3://ease-data/tpch/lineitem/partition_0.parquet', 's3://ease-data/tpch/lineitem/partition_1.parquet']) AS lineitem WHERE l_shipdate >= DATE '1994-01-01'",
        'partition_ids': [0, 1],
        'instance_id': 0,
        'metadata': {}
    }

    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
