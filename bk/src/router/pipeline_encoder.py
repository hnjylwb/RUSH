import numpy as np
import json
import os
from typing import Dict, List, Any

class PipelineEncoder:
    """
    Encodes pipeline information into feature vectors for ML prediction
    
    Encoding scheme:
    - Predefined operator set with general features for each operator:
      1. Count: number of times operator appears in pipeline
      2. Cardinality sum: sum of estimated cardinalities for this operator
      3. Data size: estimated data size based on columns and stats
    - Total feature dimension: 3 * |operator_set|
    """
    
    def __init__(self, stats_file_path: str = None):
        # Define the predefined operator set with separate phase-specific operators
        self.operator_set = [
            # Basic operators
            'SEQ_SCAN',               # Sequential table scan
            'INDEX_SCAN',             # Index scan
            'PROJECTION',             # Column projection
            'FILTER',                 # Row filtering
            'LIMIT',                  # Result limiting
            'UNION',                  # Set union
            'DISTINCT',               # Duplicate elimination
            'WINDOW',                 # Window functions
            'MATERIALIZE',            # Materialization
            'EMPTY_RESULT',           # Empty result (from filtering)
            
            # Hash join phases (different computations)
            'HASH_JOIN_BUILD',        # Hash join build phase (creating hash table)
            'HASH_JOIN_PROBE',        # Hash join probe phase (probing hash table)
            
            # Other join types
            'NESTED_LOOP_JOIN',       # Nested loop join
            'MERGE_JOIN',             # Merge join
            
            # Hash group by phases (different computations)
            'HASH_GROUP_BY_SINK',     # Hash group by sink phase (building groups)
            'HASH_GROUP_BY_SOURCE',   # Hash group by source phase (outputting groups)
            
            # Order by phases (different computations)
            'ORDER_BY_SINK',          # Order by sink phase (collecting data for sort)
            'ORDER_BY_SOURCE',        # Order by source phase (outputting sorted data)
        ]
        
        # Create operator to index mapping
        self.op_to_idx = {op: idx for idx, op in enumerate(self.operator_set)}
        
        # Feature dimension: 3 features per operator
        self.feature_dim = 3 * len(self.operator_set)
        
        # Load statistics for data size estimation
        self.stats = self._load_stats(stats_file_path)
    
    def _load_stats(self, stats_file_path: str) -> Dict:
        """Load table statistics from stats.json file"""
        if stats_file_path and os.path.exists(stats_file_path):
            try:
                with open(stats_file_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load stats from {stats_file_path}: {e}")
        
        # Return default stats with column-level information for columnar storage
        # Each column has its own average byte size (no type field needed)
        return {
            'tables': {
                'users': {
                    'columns': {
                        'id': {'avg_bytes': 8},          # INTEGER primary key
                        'name': {'avg_bytes': 25},       # VARCHAR names
                        'age': {'avg_bytes': 4}          # INTEGER age
                    }
                },
                'orders': {
                    'columns': {
                        'id': {'avg_bytes': 8},          # INTEGER primary key
                        'user_id': {'avg_bytes': 8},     # INTEGER foreign key
                        'product_id': {'avg_bytes': 8},  # INTEGER foreign key
                        'total': {'avg_bytes': 8},       # DECIMAL/DOUBLE
                        'region': {'avg_bytes': 15}      # VARCHAR region names
                    }
                },
                'products': {
                    'columns': {
                        'id': {'avg_bytes': 8},          # INTEGER primary key
                        'name': {'avg_bytes': 30},       # VARCHAR product names
                        'price': {'avg_bytes': 8},       # DECIMAL/DOUBLE
                        'category': {'avg_bytes': 20}    # VARCHAR categories
                    }
                },
                'employees': {
                    'columns': {
                        'id': {'avg_bytes': 8},          # INTEGER primary key
                        'name': {'avg_bytes': 25},       # VARCHAR names
                        'department': {'avg_bytes': 20}, # VARCHAR department names
                        'salary': {'avg_bytes': 8}       # DECIMAL/DOUBLE
                    }
                }
            }
        }
    
    def encode_pipeline(self, pipeline: Dict[str, Any]) -> np.ndarray:
        """
        Encode a single pipeline into feature vector
        
        Args:
            pipeline: Pipeline dictionary from PipelineSplitter
            
        Returns:
            Feature vector as numpy array of size (3 * |operator_set|,)
        """
        # Initialize feature vector
        features = np.zeros(self.feature_dim)
        
        operators = pipeline.get('operators', [])
        
        # Process each operator in the pipeline
        for op in operators:
            # Get operator name and phase information
            base_op_name = op.get('operator', '')
            phase = op.get('phase', None)
            
            # Normalize to phase-specific operator name
            op_name = self._normalize_operator_name(base_op_name, phase)
            
            if op_name in self.op_to_idx:
                op_idx = self.op_to_idx[op_name]
                base_idx = op_idx * 3
                
                # Feature 1: Count (increment by 1)
                features[base_idx] += 1
                
                # Feature 2: Cardinality sum
                cardinality = op.get('estimated_cardinality', 0)
                features[base_idx + 1] += cardinality
                
                # Feature 3: Data size estimation
                data_size = self._estimate_data_size(op, cardinality)
                features[base_idx + 2] += data_size
        
        return features
    
    def _normalize_operator_name(self, op_name: str, phase: str = None) -> str:
        """
        Normalize operator name to match predefined operator set with phase information
        
        Args:
            op_name: Base operator name (e.g., 'HASH_JOIN', 'HASH_GROUP_BY')
            phase: Phase information (e.g., 'build', 'probe', 'sink', 'source')
        
        For operators with phases, we map them to phase-specific operator names:
        - HASH_JOIN + build -> HASH_JOIN_BUILD
        - HASH_JOIN + probe -> HASH_JOIN_PROBE  
        - HASH_GROUP_BY + sink -> HASH_GROUP_BY_SINK
        - HASH_GROUP_BY + source -> HASH_GROUP_BY_SOURCE
        - ORDER_BY + sink -> ORDER_BY_SINK
        - ORDER_BY + source -> ORDER_BY_SOURCE
        """
        op_upper = op_name.upper().strip()
        
        # If no phase provided, try to extract from op_name (legacy support)
        if phase is None and '(' in op_upper:
            parts = op_upper.split('(')
            op_upper = parts[0]
            if len(parts) > 1:
                phase = parts[1].rstrip(')').strip()
        
        # Handle phase-specific operators
        if op_upper == 'HASH_JOIN':
            if phase == 'build':
                return 'HASH_JOIN_BUILD'
            elif phase == 'probe':
                return 'HASH_JOIN_PROBE'
            else:
                # If no phase specified, default to build (shouldn't happen with our pipeline splitter)
                return 'HASH_JOIN_BUILD'
        
        elif op_upper == 'HASH_GROUP_BY':
            if phase == 'sink':
                return 'HASH_GROUP_BY_SINK'
            elif phase == 'source':
                return 'HASH_GROUP_BY_SOURCE'
            else:
                # If no phase specified, default to sink
                return 'HASH_GROUP_BY_SINK'
        
        elif op_upper == 'ORDER_BY':
            if phase == 'sink':
                return 'ORDER_BY_SINK'
            elif phase == 'source':
                return 'ORDER_BY_SOURCE'
            else:
                # If no phase specified, default to sink
                return 'ORDER_BY_SINK'
        
        # Direct mapping for operators already in our set
        if op_upper in self.operator_set:
            return op_upper
        
        # Handle variations and aliases
        mapping = {
            'TABLE_SCAN': 'SEQ_SCAN',
            'SCAN': 'SEQ_SCAN',
            'READ_PARQUET': 'SEQ_SCAN',
            'PROJECT': 'PROJECTION',
            'SELECT': 'FILTER',
            'WHERE': 'FILTER',
            'SORT': 'ORDER_BY_SINK',  # Default to sink phase
            'AGGREGATE': 'HASH_GROUP_BY_SINK',  # Default to sink phase
            'GROUP_BY': 'HASH_GROUP_BY_SINK',  # Default to sink phase
            'HASH_AGGREGATE': 'HASH_GROUP_BY_SINK',  # Default to sink phase
            'JOIN': 'HASH_JOIN_BUILD',  # Default to build phase
            'NL_JOIN': 'NESTED_LOOP_JOIN',
            'MERGE_SORT_JOIN': 'MERGE_JOIN'
        }
        
        # Check for direct mapping
        if op_upper in mapping:
            return mapping[op_upper]
        
        # Check for partial matches
        for alias, standard in mapping.items():
            if alias in op_upper:
                return standard
        
        # Special handling for scans
        if 'SCAN' in op_upper:
            if 'INDEX' in op_upper:
                return 'INDEX_SCAN'
            else:
                return 'SEQ_SCAN'
        
        # Special handling for joins (without phase info)
        if 'JOIN' in op_upper:
            if 'HASH' in op_upper:
                return 'HASH_JOIN_BUILD'  # Default to build phase
            elif 'NESTED' in op_upper or 'NL' in op_upper:
                return 'NESTED_LOOP_JOIN'
            elif 'MERGE' in op_upper:
                return 'MERGE_JOIN'
            else:
                return 'HASH_JOIN_BUILD'  # Default to hash join build
        
        # If no match found, return as-is (might not be encoded)
        return op_upper
    
    def _estimate_data_size(self, operator: Dict[str, Any], cardinality: int) -> float:
        """
        Estimate data size for an operator based on cardinality and column-level statistics
        
        For columnar storage (DuckDB), data size depends on which columns are accessed
        rather than entire row size.
        
        Args:
            operator: Operator information
            cardinality: Estimated cardinality from optimizer
            
        Returns:
            Estimated data size in bytes
        """
        op_name = operator.get('operator', '')
        
        # Extract table information from operator extra_info if available
        table_name = self._extract_table_name(operator)
        accessed_columns = self._extract_accessed_columns(operator)
        
        if table_name and table_name in self.stats.get('tables', {}):
            table_stats = self.stats['tables'][table_name]
            columns_info = table_stats.get('columns', {})
            
            # Calculate size based on accessed columns
            if accessed_columns:
                # Use specific columns that are accessed
                bytes_per_row = sum(
                    columns_info.get(col, {}).get('avg_bytes', 8) 
                    for col in accessed_columns 
                    if col in columns_info
                )
            else:
                # If we can't determine specific columns, estimate based on operator type
                if 'SCAN' in op_name.upper():
                    # For scans, assume all columns are accessed initially
                    bytes_per_row = sum(col_info.get('avg_bytes', 8) for col_info in columns_info.values())
                elif 'PROJECTION' in op_name.upper():
                    # For projections, assume subset of columns (estimated 60% on average)
                    total_size = sum(col_info.get('avg_bytes', 8) for col_info in columns_info.values())
                    bytes_per_row = total_size * 0.6
                elif 'JOIN' in op_name.upper():
                    # For joins, assume columns from multiple tables (estimate based on join keys)
                    # Use average column size times estimated number of columns in join result
                    avg_col_size = sum(col_info.get('avg_bytes', 8) for col_info in columns_info.values()) / max(len(columns_info), 1)
                    estimated_result_columns = len(columns_info) * 1.5  # Join typically combines columns
                    bytes_per_row = avg_col_size * estimated_result_columns
                elif 'GROUP' in op_name.upper() or 'AGGREGATE' in op_name.upper():
                    # For aggregations, output typically has group keys + aggregate values
                    # Estimate: ~3 columns on average (group key + 1-2 aggregates)
                    avg_col_size = sum(col_info.get('avg_bytes', 8) for col_info in columns_info.values()) / max(len(columns_info), 1)
                    bytes_per_row = avg_col_size * 3
                else:
                    # Default: assume moderate column access
                    total_size = sum(col_info.get('avg_bytes', 8) for col_info in columns_info.values())
                    bytes_per_row = total_size * 0.5
            
            return float(cardinality * bytes_per_row)
        
        else:
            # Default estimation when no table stats available
            # Use conservative estimate: 4 columns * 8 bytes each = 32 bytes per row
            default_bytes_per_row = 32
            return float(cardinality * default_bytes_per_row)
    
    def _extract_table_name(self, operator: Dict[str, Any]) -> str:
        """
        Extract table name from operator extra_info
        
        Args:
            operator: Operator information
            
        Returns:
            Table name if found, empty string otherwise
        """
        extra_info = operator.get('extra_info', {})
        
        # Check common fields where table name might be stored
        for field in ['Table Name', 'table_name', 'relation', 'table']:
            if field in extra_info:
                return str(extra_info[field]).lower()
        
        # Try to parse from other info
        if 'info' in extra_info:
            info_str = str(extra_info['info']).lower()
            # Look for table names we know about
            for table in ['users', 'orders', 'products', 'employees']:
                if table in info_str:
                    return table
        
        return ''
    
    def _extract_accessed_columns(self, operator: Dict[str, Any]) -> List[str]:
        """
        Extract accessed columns from operator extra_info
        
        Args:
            operator: Operator information
            
        Returns:
            List of column names that are accessed by this operator
        """
        extra_info = operator.get('extra_info', {})
        columns = []
        
        # Check for projection information (columns being selected)
        if 'Projections' in extra_info:
            projections = extra_info['Projections']
            if isinstance(projections, list):
                columns.extend(projections)
            elif isinstance(projections, str):
                # Parse projection string, e.g., "id, name, age"
                columns.extend([col.strip() for col in projections.split(',')])
        
        # Check for filter conditions (columns being filtered)
        if 'Filters' in extra_info:
            filters = extra_info['Filters']
            if isinstance(filters, str):
                # Simple parsing: extract column names from filter expressions
                # This is a heuristic and may need refinement based on actual DuckDB output
                import re
                # Find column-like patterns (alphanumeric + underscore, not starting with digit)
                potential_cols = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', filters)
                # Filter out common SQL keywords
                sql_keywords = {'AND', 'OR', 'NOT', 'TRUE', 'FALSE', 'NULL', 'IS', 'IN', 'LIKE'}
                for col in potential_cols:
                    if col.upper() not in sql_keywords and not col.isdigit():
                        columns.append(col)
        
        # Check for join conditions (columns used in joins)
        if 'Join Condition' in extra_info:
            join_condition = extra_info['Join Condition']
            if isinstance(join_condition, str):
                # Extract column names from join condition
                import re
                potential_cols = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', join_condition)
                sql_keywords = {'AND', 'OR', 'ON', 'USING'}
                for col in potential_cols:
                    if col.upper() not in sql_keywords and not col.isdigit():
                        columns.append(col)
        
        # Check for group by columns
        if 'Groups' in extra_info:
            groups = extra_info['Groups']
            if isinstance(groups, list):
                columns.extend(groups)
            elif isinstance(groups, str):
                columns.extend([col.strip() for col in groups.split(',')])
        
        # Remove duplicates and return
        return list(set(columns))
    
    def encode_pipelines(self, pipelines: List[Dict[str, Any]]) -> np.ndarray:
        """
        Encode multiple pipelines into feature matrix
        
        Args:
            pipelines: List of pipeline dictionaries
            
        Returns:
            Feature matrix of shape (len(pipelines), feature_dim)
        """
        if not pipelines:
            return np.zeros((0, self.feature_dim))
        
        feature_matrix = np.zeros((len(pipelines), self.feature_dim))
        
        for i, pipeline in enumerate(pipelines):
            feature_matrix[i] = self.encode_pipeline(pipeline)
        
        return feature_matrix
    
    def get_feature_names(self) -> List[str]:
        """
        Get human-readable feature names for debugging
        
        Returns:
            List of feature names
        """
        feature_names = []
        
        for op in self.operator_set:
            feature_names.extend([
                f"{op}_count",
                f"{op}_cardinality_sum", 
                f"{op}_data_size"
            ])
        
        return feature_names