import duckdb
import os
from typing import Dict, Any

class QueryParser:
    """
    Uses DuckDB to parse SQL queries into execution plans
    """
    
    def __init__(self):
        self.conn = None
        self._initialize_connection()
        self._setup_test_data()
    
    def _initialize_connection(self):
        """Initialize DuckDB connection"""
        try:
            self.conn = duckdb.connect()
            print("DuckDB connection initialized for query parsing")
        except Exception as e:
            print(f"Failed to initialize DuckDB: {e}")
            self.conn = None
    
    def _setup_test_data(self):
        """Set up test data tables from CSV files"""
        if not self.conn:
            return
            
        try:
            # Get the project root directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.join(current_dir, '..', '..')
            test_data_dir = os.path.join(project_root, 'test_data')
            
            # Create tables from CSV files
            csv_files = {
                'users': os.path.join(test_data_dir, 'users.csv'),
                'orders': os.path.join(test_data_dir, 'orders.csv'),
                'products': os.path.join(test_data_dir, 'products.csv'),
                'employees': os.path.join(test_data_dir, 'employees.csv')
            }
            
            for table_name, csv_path in csv_files.items():
                if os.path.exists(csv_path):
                    # Create table from CSV
                    create_query = f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
                    self.conn.execute(create_query)
                    print(f"Created table '{table_name}' from {csv_path}")
                else:
                    print(f"Warning: CSV file not found: {csv_path}")
                    
        except Exception as e:
            print(f"Failed to setup test data: {e}")
            # Continue without test data - will use fallback parsing
    
    def parse_query(self, sql: str) -> Dict[str, Any]:
        """
        Parse SQL query into execution plan using DuckDB
        
        Returns:
            Dict containing parsed execution plan with operators and metadata
        """
        if not self.conn:
            raise Exception("DuckDB connection not available")
        
        try:
            # Get execution plan without executing the query
            explain_query = f"EXPLAIN (FORMAT JSON) {sql}"
            result = self.conn.execute(explain_query).fetchone()
            
            if result and len(result) > 1 and result[1]:
                # Parse JSON plan (second element of tuple) and return directly
                import json
                return json.loads(result[1])
            else:
                raise Exception(f"DuckDB EXPLAIN (FORMAT JSON) returned empty result for query: {sql}")
                
        except Exception as e:
            # Don't fallback - let the error propagate so we know what's wrong
            raise Exception(f"DuckDB JSON parsing failed for query '{sql}': {e}") from e
    
    def close(self):
        """Close DuckDB connection"""
        if self.conn:
            self.conn.close()
            self.conn = None