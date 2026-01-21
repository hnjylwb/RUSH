"""
DuckDB EXPLAIN Plan Parser

Parses DuckDB EXPLAIN (FORMAT JSON) output to extract features for cost estimation.

DuckDB is used because:
1. Already integrated in EASE for resource prediction
2. Column-store architecture similar to Athena/QaaS
3. Clean JSON EXPLAIN format
4. Fast local execution
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import json
import duckdb


@dataclass
class PlanNode:
    """
    Represents a node in the query execution plan

    Attributes:
        operator_type: Type of operator (e.g., "TABLE_SCAN", "HASH_JOIN")
        operator_name: Name of operator (usually same as type)
        cardinality: Estimated number of rows
        actual_cardinality: Actual rows (if ANALYZE was used)
        timing: Execution time in seconds (if ANALYZE was used)
        extra_info: Additional operator-specific information
        children: Child plan nodes
    """
    operator_type: str
    operator_name: str
    cardinality: int  # Estimated cardinality
    actual_cardinality: Optional[int] = None  # From EXPLAIN ANALYZE
    timing: Optional[float] = None  # From EXPLAIN ANALYZE
    extra_info: Dict[str, Any] = None
    children: List['PlanNode'] = None

    def __post_init__(self):
        if self.extra_info is None:
            self.extra_info = {}
        if self.children is None:
            self.children = []


@dataclass
class ParsedPlan:
    """
    Complete parsed query execution plan

    Attributes:
        root: Root node of the plan tree
        sql: Original SQL query
        total_cardinality: Total estimated cardinality
        scan_nodes: List of scan/table access nodes
        join_nodes: List of join nodes
        tables: Set of tables involved
        runtime: Query runtime (if ANALYZE was used)
    """
    root: PlanNode
    sql: str
    total_cardinality: int
    scan_nodes: List[PlanNode]
    join_nodes: List[PlanNode]
    tables: List[str]
    runtime: Optional[float] = None


class PlanParser:
    """
    Parser for DuckDB EXPLAIN plans

    Parses JSON-formatted EXPLAIN output from DuckDB to extract features
    for cost estimation models.

    Usage:
        parser = PlanParser()

        # Get EXPLAIN plan from DuckDB
        conn = duckdb.connect('database.db')
        result = conn.execute('EXPLAIN (FORMAT JSON) ' + sql).fetchone()
        plan_json = json.loads(result[1])

        # Parse the plan
        parsed_plan = parser.parse_plan(plan_json, sql)

        # Access features
        print(f"Tables: {parsed_plan.tables}")
        print(f"Scans: {len(parsed_plan.scan_nodes)}")
        print(f"Joins: {len(parsed_plan.join_nodes)}")
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize plan parser

        Args:
            config: Optional configuration
        """
        self.config = config or {}

        # Operator types that represent table scans
        self.scan_operators = {
            'TABLE_SCAN', 'SEQ_SCAN', 'INDEX_SCAN',
            'PARQUET_SCAN', 'CSV_SCAN'
        }

        # Operator types that represent joins
        self.join_operators = {
            'HASH_JOIN', 'NESTED_LOOP_JOIN', 'MERGE_JOIN',
            'CROSS_PRODUCT', 'POSITIONAL_JOIN'
        }

    def parse_plan(
        self,
        plan_json: Any,  # Can be Dict or List
        sql: str,
        is_analyze: bool = False
    ) -> ParsedPlan:
        """
        Parse DuckDB EXPLAIN plan from JSON

        Args:
            plan_json: JSON object/list from EXPLAIN output
                      (DuckDB returns a list with one element)
            sql: Original SQL query
            is_analyze: Whether this is from EXPLAIN ANALYZE

        Returns:
            ParsedPlan object with extracted features
        """
        # Handle list format (DuckDB EXPLAIN returns a list)
        if isinstance(plan_json, list):
            if len(plan_json) == 0:
                raise ValueError("Empty plan JSON")
            plan_json = plan_json[0]

        # Parse the plan tree
        root = self._parse_node(plan_json)

        # Extract scan and join nodes
        scan_nodes = []
        join_nodes = []
        tables = []

        self._extract_nodes(root, scan_nodes, join_nodes, tables)

        # Get total cardinality (from root node)
        total_cardinality = root.cardinality

        # Get runtime if available (from EXPLAIN ANALYZE)
        runtime = None
        if is_analyze and 'latency' in plan_json:
            runtime = plan_json['latency']

        return ParsedPlan(
            root=root,
            sql=sql,
            total_cardinality=total_cardinality,
            scan_nodes=scan_nodes,
            join_nodes=join_nodes,
            tables=list(set(tables)),  # Remove duplicates
            runtime=runtime
        )

    def _parse_node(self, node_json: Dict[str, Any]) -> PlanNode:
        """
        Recursively parse a plan node from JSON

        Args:
            node_json: JSON object representing a plan node

        Returns:
            PlanNode object
        """
        # Handle EXPLAIN ANALYZE format (has extra wrapper)
        if 'children' in node_json and len(node_json.get('children', [])) > 0:
            # Check if this is the top-level EXPLAIN_ANALYZE node
            if node_json.get('operator_type') == 'EXPLAIN_ANALYZE':
                # Skip the EXPLAIN_ANALYZE wrapper and parse the actual query plan
                return self._parse_node(node_json['children'][0])

        # Extract basic info
        # EXPLAIN uses 'name', EXPLAIN ANALYZE uses 'operator_type'
        operator_type = node_json.get('operator_type') or node_json.get('name', 'UNKNOWN')
        operator_name = node_json.get('operator_name') or node_json.get('name', 'UNKNOWN')

        # Normalize operator type (remove trailing spaces)
        operator_type = operator_type.strip()
        operator_name = operator_name.strip()

        # Get cardinality
        extra_info = node_json.get('extra_info', {})
        cardinality = int(extra_info.get('Estimated Cardinality', 0))

        # Get actual cardinality and timing (from EXPLAIN ANALYZE)
        actual_cardinality = node_json.get('operator_cardinality')
        timing = node_json.get('operator_timing')

        # Parse children recursively
        children = []
        for child_json in node_json.get('children', []):
            children.append(self._parse_node(child_json))

        return PlanNode(
            operator_type=operator_type,
            operator_name=operator_name,
            cardinality=cardinality,
            actual_cardinality=actual_cardinality,
            timing=timing,
            extra_info=extra_info,
            children=children
        )

    def _extract_nodes(
        self,
        node: PlanNode,
        scan_nodes: List[PlanNode],
        join_nodes: List[PlanNode],
        tables: List[str]
    ):
        """
        Recursively extract scan and join nodes from the plan tree

        Args:
            node: Current plan node
            scan_nodes: List to collect scan nodes
            join_nodes: List to collect join nodes
            tables: List to collect table names
        """
        # Check if this is a scan node
        if node.operator_type in self.scan_operators:
            scan_nodes.append(node)
            # Extract table name
            if 'Table' in node.extra_info:
                tables.append(node.extra_info['Table'])

        # Check if this is a join node
        elif node.operator_type in self.join_operators:
            join_nodes.append(node)

        # Recurse on children
        for child in node.children:
            self._extract_nodes(child, scan_nodes, join_nodes, tables)

    def get_plan_from_duckdb(
        self,
        sql: str,
        db_path: str = ':memory:',
        analyze: bool = False
    ) -> Tuple[Dict[str, Any], Optional[duckdb.DuckDBPyConnection]]:
        """
        Execute EXPLAIN on DuckDB and get the plan JSON

        Args:
            sql: SQL query to explain
            db_path: Path to DuckDB database (default: in-memory)
            analyze: Whether to use EXPLAIN ANALYZE (actually runs the query)

        Returns:
            Tuple of (plan JSON, connection)
            The connection is returned so caller can close it
        """
        try:
            conn = duckdb.connect(db_path)

            # Build EXPLAIN command
            if analyze:
                explain_sql = f'EXPLAIN (FORMAT JSON, ANALYZE) {sql}'
            else:
                explain_sql = f'EXPLAIN (FORMAT JSON) {sql}'

            # Execute and get result
            result = conn.execute(explain_sql).fetchone()

            # Parse JSON
            if result and len(result) >= 2:
                plan_json = json.loads(result[1])
                return plan_json, conn
            else:
                return None, conn

        except Exception as e:
            print(f"Error getting plan from DuckDB: {e}")
            return None, None

    def parse_from_duckdb(
        self,
        sql: str,
        db_path: str = ':memory:',
        analyze: bool = False
    ) -> Optional[ParsedPlan]:
        """
        Convenience method: Get plan from DuckDB and parse it

        Args:
            sql: SQL query to explain
            db_path: Path to DuckDB database
            analyze: Whether to use EXPLAIN ANALYZE

        Returns:
            ParsedPlan object or None if parsing fails
        """
        plan_json, conn = self.get_plan_from_duckdb(sql, db_path, analyze)

        try:
            if plan_json:
                parsed = self.parse_plan(plan_json, sql, is_analyze=analyze)
                return parsed
            return None
        finally:
            if conn:
                conn.close()
