"""
Feature extraction for QaaS cost model predictions

Extracts features from query resource requirements and SQL queries for regression-based prediction.
Supports multiple levels of feature extraction:
1. Basic: Uses only data scanned (simple, available from resource requirements)
2. SQL-based: Parses SQL to extract structural features (num_tables, num_joins)
3. Plan-based: Parses EXPLAIN plan for detailed features (requires database connection)
"""

from typing import List, Dict, Optional
import numpy as np
import duckdb
import json
from .core import ResourceRequirements
from .ml import QueryParser, FeatureBuilder, PlanParser


class QaaSFeatureExtractor:
    """
    Feature extractor for QaaS (Query as a Service) cost estimation

    Three levels of feature extraction:

    1. **Basic**: Uses data_scanned_mb from ResourceRequirements
       - Simple, fast, works with any client
       - Feature: InputBytes in MB
       - Use case: Simple linear regression baseline

    2. **SQL-based (Phase 1 - Current)**: Parses SQL to extract structural features
       - Extracts: num_tables, num_joins
       - No database connection needed
       - Use case: Enhanced regression models

    3. **Plan-based (Phase 2 - Future)**: Parses EXPLAIN plan for detailed features
       - Requires EXPLAIN plan from database
       - Graph neural network based approach
       - Features include:
         * ENCODE_FEATURES: num_tables, num_joins
         * SCAN_FEATURES: cardinality, width, children_card
         * JOIN_FEATURES: cardinality, width, children_card
         * FILTER_FEATURES: operator, literal_feature
         * COLUMN_FEATURES: avg_width, correlation, data_type, n_distinct, null_frac
         * TABLE_FEATURES: reltuples, relcols
       - Use case: Sophisticated GNN-based prediction
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize feature extractor

        Args:
            config: Optional configuration for feature extraction
        """
        self.config = config or {}
        self.query_parser = QueryParser(config)
        self.feature_builder = FeatureBuilder(config)
        self.plan_parser = PlanParser()

        # DuckDB database path for EXPLAIN (if plan-based features are enabled)
        self.db_path = config.get('duckdb_schema_path', ':memory:')
        self.use_plan_features = config.get('use_plan_features', False)

        # Feature cache for performance
        self._feature_cache = {}

    def extract_features(self, resources: ResourceRequirements, sql: Optional[str] = None) -> List[float]:
        """
        Extract features from ResourceRequirements for QaaS prediction

        Returns 21-dimensional feature vector when plan features are enabled:
        1. data_scanned_mb (1 feature)
        2. num_tables, num_joins (2 features - ENCODE)
        3. scan aggregates (9 features - sum/max/avg of cardinality/width/children_card)
        4. join aggregates (9 features - sum/max/avg of cardinality/width/children_card)

        If plan features are disabled or SQL is not provided, returns only data_scanned_mb
        with zeros for missing features.

        Args:
            resources: Query resource requirements
            sql: Optional SQL query string for plan feature extraction

        Returns:
            Feature vector (1 or 21 dimensions depending on configuration)
        """
        # Always extract data_scanned feature
        data_scanned_mb = resources.total_data_scanned / (1024 * 1024)
        features = [float(data_scanned_mb)]

        # If plan features are disabled, return only data_scanned
        if not self.use_plan_features:
            return features

        # Check cache for SQL-based features
        if sql and sql in self._feature_cache:
            cached_plan_features = self._feature_cache[sql]
            features.extend(cached_plan_features)
            return features

        # If SQL not provided, return with zeros for plan features
        if sql is None:
            features.extend([0.0] * 20)
            return features

        # Extract plan-based features
        try:
            # Get DuckDB EXPLAIN plan
            conn = duckdb.connect(self.db_path)
            result = conn.execute(f'EXPLAIN (FORMAT JSON) {sql}').fetchone()

            if result:
                plan_json = json.loads(result[1])

                # Parse plan
                parsed_plan = self.plan_parser.parse_plan(plan_json, sql)

                # Extract ENCODE features
                num_tables = float(len(parsed_plan.tables))
                num_joins = float(len(parsed_plan.join_nodes))
                plan_features = [num_tables, num_joins]

                # Extract and aggregate SCAN features
                scan_features = self.feature_builder.extract_scan_features_from_plan(parsed_plan)

                if scan_features:
                    scan_array = np.array(scan_features)
                    scan_sum = scan_array.sum(axis=0)
                    scan_max = scan_array.max(axis=0)
                    scan_avg = scan_array.mean(axis=0)

                    plan_features.extend([
                        scan_sum[0], scan_sum[1], scan_sum[2],
                        scan_max[0], scan_max[1], scan_max[2],
                        scan_avg[0], scan_avg[1], scan_avg[2],
                    ])
                else:
                    # No scans - add zeros
                    plan_features.extend([0.0] * 9)

                # Extract and aggregate JOIN features
                join_features = self.feature_builder.extract_join_features_from_plan(parsed_plan)

                if join_features:
                    join_array = np.array(join_features)
                    join_sum = join_array.sum(axis=0)
                    join_max = join_array.max(axis=0)
                    join_avg = join_array.mean(axis=0)

                    plan_features.extend([
                        join_sum[0], join_sum[1], join_sum[2],
                        join_max[0], join_max[1], join_max[2],
                        join_avg[0], join_avg[1], join_avg[2],
                    ])
                else:
                    # No joins - add zeros
                    plan_features.extend([0.0] * 9)

                # Cache the plan features
                self._feature_cache[sql] = plan_features

                # Combine with data_scanned
                features.extend(plan_features)

            conn.close()

        except Exception as e:
            print(f"Warning: Failed to extract plan features: {e}")
            # Fallback: return data_scanned with zeros for plan features
            features.extend([0.0] * 20)

        return features

    def get_feature_names(self) -> List[str]:
        """
        Get feature names for interpretability

        Returns:
            List of feature names (1 or 21 features depending on configuration)
        """
        base_features = ["data_scanned_mb"]

        if not self.use_plan_features:
            return base_features

        plan_features = [
            "num_tables", "num_joins",
            "scan_sum_cardinality", "scan_sum_width", "scan_sum_children_card",
            "scan_max_cardinality", "scan_max_width", "scan_max_children_card",
            "scan_avg_cardinality", "scan_avg_width", "scan_avg_children_card",
            "join_sum_cardinality", "join_sum_width", "join_sum_children_card",
            "join_max_cardinality", "join_max_width", "join_max_children_card",
            "join_avg_cardinality", "join_avg_width", "join_avg_children_card",
        ]

        return base_features + plan_features

    def get_num_features(self) -> int:
        """
        Get number of features extracted

        Returns:
            Number of features
        """
        return len(self.get_feature_names())
