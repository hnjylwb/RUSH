"""
Query Parser for extracting features from SQL queries

Parses SQL queries and their EXPLAIN plans to extract features for cost estimation.
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import re


@dataclass
class ParsedQuery:
    """
    Parsed query representation with extracted features

    This data structure holds all information extracted from a SQL query
    and its EXPLAIN plan, ready for feature extraction and model inference.

    Attributes:
        sql: Original SQL query string
        num_tables: Number of tables in the query
        num_joins: Number of join operations
        scan_nodes: List of scan node information
        join_nodes: List of join node information
        filter_nodes: List of filter conditions
        tables: Set of table names involved
        plan_runtime: Actual runtime (for training) or None (for inference)
    """
    sql: str
    num_tables: int
    num_joins: int
    scan_nodes: List[Dict[str, Any]]
    join_nodes: List[Dict[str, Any]]
    filter_nodes: Dict[str, Any]
    tables: List[str]
    plan_runtime: Optional[float] = None
    database_id: int = 0


class QueryParser:
    """
    SQL Query Parser

    Parses SQL queries to extract structural features needed for cost estimation.

    Current implementation (Phase 1 - Simplified):
    - Extracts basic features: num_tables, num_joins
    - Does NOT require EXPLAIN plan (uses regex-based SQL parsing)
    - Does NOT require database connection

    Future implementation (Phase 2 - Advanced):
    - Parse EXPLAIN plan to get cardinality, width, etc.
    - Build graph structure for GNN model
    - Extract all features for sophisticated cost estimation

    Usage:
        parser = QueryParser()
        parsed = parser.parse_query(sql)
        print(f"Tables: {parsed.num_tables}, Joins: {parsed.num_joins}")
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize query parser

        Args:
            config: Optional configuration
        """
        self.config = config or {}

    def parse_query(self, sql: str) -> Optional[ParsedQuery]:
        """
        Parse SQL query to extract features

        Current implementation: Simple regex-based parsing
        - Counts tables in FROM clause
        - Counts JOIN keywords

        Args:
            sql: SQL query string

        Returns:
            ParsedQuery object with extracted features, or None if parsing fails
        """
        try:
            # Normalize SQL: convert to uppercase for pattern matching
            sql_upper = sql.upper()

            # Extract num_tables: count table names in FROM clause
            num_tables = self._count_tables(sql_upper)

            # Extract num_joins: count JOIN keywords
            num_joins = self._count_joins(sql_upper)

            # Extract table names
            tables = self._extract_table_names(sql_upper)

            # Create ParsedQuery object with extracted features
            # Note: scan_nodes, join_nodes, filter_nodes are empty in Phase 1
            # They will be populated in Phase 2 when we parse EXPLAIN plans
            parsed = ParsedQuery(
                sql=sql,
                num_tables=num_tables,
                num_joins=num_joins,
                scan_nodes=[],
                join_nodes=[],
                filter_nodes={},
                tables=list(tables),
            )

            return parsed

        except Exception as e:
            print(f"Warning: Failed to parse query: {e}")
            return None

    def _count_tables(self, sql: str) -> int:
        """
        Count number of tables in FROM clause

        Args:
            sql: SQL query (uppercase)

        Returns:
            Number of tables
        """
        # Extract FROM clause with word boundaries to avoid matching table names
        from_match = re.search(r'FROM\s+(.*?)(?:\sWHERE\s|\sGROUP\s|\sORDER\s|\sLIMIT\s|\sJOIN\s|$)', sql, re.IGNORECASE | re.DOTALL)
        if not from_match:
            return 0

        from_clause = from_match.group(1).strip()

        # Count commas (indicates multiple tables) + 1
        # This is a simplified approach; more robust parsing would use an SQL parser
        num_commas = from_clause.count(',')
        return num_commas + 1

    def _count_joins(self, sql: str) -> int:
        """
        Count number of JOIN operations

        Args:
            sql: SQL query (uppercase)

        Returns:
            Number of joins
        """
        # Count all types of JOINs
        join_patterns = [
            r'\bJOIN\b',
            r'\bINNER\s+JOIN\b',
            r'\bLEFT\s+JOIN\b',
            r'\bRIGHT\s+JOIN\b',
            r'\bFULL\s+JOIN\b',
            r'\bCROSS\s+JOIN\b',
        ]

        num_joins = 0
        for pattern in join_patterns:
            # Count non-overlapping occurrences
            matches = re.findall(pattern, sql, re.IGNORECASE)
            if pattern == r'\bJOIN\b':
                # Don't count if it's part of "INNER JOIN", "LEFT JOIN", etc.
                # We already counted those
                for match in matches:
                    if not any(prefix in sql[max(0, sql.find(match) - 10):sql.find(match)]
                              for prefix in ['INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS']):
                        num_joins += 1
            else:
                num_joins += len(matches)

        return num_joins

    def _extract_table_names(self, sql: str) -> List[str]:
        """
        Extract table names from SQL query

        Args:
            sql: SQL query (uppercase)

        Returns:
            List of table names
        """
        tables = []

        # Extract from FROM clause with word boundaries
        from_match = re.search(r'FROM\s+(.*?)(?:\sWHERE\s|\sGROUP\s|\sORDER\s|\sLIMIT\s|\sJOIN\s|$)', sql, re.IGNORECASE | re.DOTALL)
        if from_match:
            from_clause = from_match.group(1).strip()
            # Split by comma and extract table names (before AS keyword if present)
            for table_expr in from_clause.split(','):
                table_expr = table_expr.strip()
                # Remove alias (everything after AS)
                table_name = re.split(r'\s+AS\s+|\s+', table_expr, maxsplit=1)[0].strip()
                if table_name:
                    tables.append(table_name)

        # Extract from JOIN clauses
        join_matches = re.finditer(r'JOIN\s+(\w+)', sql, re.IGNORECASE)
        for match in join_matches:
            table_name = match.group(1)
            if table_name not in tables:
                tables.append(table_name)

        return tables
