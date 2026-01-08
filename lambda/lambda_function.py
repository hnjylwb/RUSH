"""
Lambda Function Handler for Query Execution

This is the Lambda function that runs on AWS Lambda (or similar FaaS platform).
Each Lambda instance processes a subset of data partitions.
"""

import json
import time
from typing import Dict, List, Any


def rewrite_sql_for_partitions(sql: str, partition_ids: List[int],
                               data_location: str) -> str:
    """
    Rewrite SQL query to read from specific partitions

    This function transforms the original SQL to read only from assigned partitions.
    TODO: Implement proper SQL rewriting logic.

    Args:
        sql: Original SQL query
        partition_ids: List of partition IDs to process
        data_location: Base path to data (e.g., s3://bucket/table)

    Returns:
        Rewritten SQL that reads from partitions
    """
    # TODO: Implement SQL parser and rewriter
    # Current simple implementation:
    # - Build partition file list
    # - Replace table references with read_parquet()

    # Build partition file paths
    partition_files = [f"{data_location}/partition_{pid}.parquet" for pid in partition_ids]

    # Create DuckDB read expression
    if len(partition_files) == 1:
        table_expr = f"read_parquet('{partition_files[0]}')"
    else:
        files_str = ", ".join([f"'{f}'" for f in partition_files])
        table_expr = f"read_parquet([{files_str}])"

    # Simple replacement (TODO: proper SQL parsing)
    # This assumes query has 'FROM table_name'
    rewritten_sql = sql.replace('FROM lineitem', f'FROM ({table_expr})')
    rewritten_sql = rewritten_sql.replace('FROM orders', f'FROM ({table_expr})')
    # TODO: Handle more tables and complex queries

    return rewritten_sql


def execute_query_on_partitions(sql: str, partition_ids: List[int],
                                data_location: str) -> Dict[str, Any]:
    """
    Execute query on specified partitions using DuckDB

    Args:
        sql: SQL query to execute
        partition_ids: List of partition IDs to process
        data_location: S3 path or data location

    Returns:
        Dict with execution results
    """
    try:
        import duckdb

        # Create DuckDB connection
        conn = duckdb.connect(':memory:')

        # Rewrite SQL for partitions
        rewritten_sql = rewrite_sql_for_partitions(sql, partition_ids, data_location)

        # Execute query
        start_time = time.time()
        result = conn.execute(rewritten_sql)
        rows = result.fetchall()
        execution_time = time.time() - start_time

        conn.close()

        return {
            'status': 'success',
            'execution_time': execution_time,
            'row_count': len(rows),
            'partition_ids': partition_ids,
            'result_preview': str(rows[:3]) if rows else None
        }

    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'partition_ids': partition_ids
        }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function

    Event format:
    {
        "query_id": "q1",
        "sql": "SELECT * FROM table WHERE ...",
        "partition_ids": [0, 1, 2],
        "instance_id": 0,
        "metadata": {
            "data_location": "s3://bucket/data/table"
        }
    }

    Args:
        event: Lambda event containing query details
        context: Lambda context

    Returns:
        Execution result
    """
    try:
        query_id = event.get('query_id')
        sql = event.get('sql')
        partition_ids = event.get('partition_ids', [])
        instance_id = event.get('instance_id', 0)
        metadata = event.get('metadata', {})

        # Get data location from metadata or environment
        data_location = metadata.get('data_location', 's3://rush-data/tpch')

        print(f"[Lambda {instance_id}] Query {query_id}: Processing partitions {partition_ids}")

        # Execute query on assigned partitions
        result = execute_query_on_partitions(sql, partition_ids, data_location)

        # Add instance info
        result['instance_id'] = instance_id
        result['query_id'] = query_id

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
    # Test event
    test_event = {
        'query_id': 'test_q1',
        'sql': 'SELECT COUNT(*) FROM lineitem WHERE l_shipdate >= DATE \'1994-01-01\'',
        'partition_ids': [0, 1],
        'instance_id': 0,
        'metadata': {
            'data_location': '/data/tpch'
        }
    }

    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2))
