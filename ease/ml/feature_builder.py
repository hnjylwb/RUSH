"""
Feature Builder for constructing feature vectors from parsed queries

Converts parsed query structures into feature vectors suitable for ML models.

References:
    - bk/brad/src/brad/cost_model/dataset/query_featurization/athena_query_featurization.py
"""

from typing import List, Dict, Any, Optional, TYPE_CHECKING
from .query_parser import ParsedQuery

if TYPE_CHECKING:
    from .plan_parser import ParsedPlan, PlanNode


class FeatureBuilder:
    """
    Feature Builder for cost estimation

    Constructs feature vectors from parsed queries for use in ML models.

    Feature Sets (matching BRAD's Athena featurization):
    - ENCODE_FEATURES: ["num_tables", "num_joins"]
    - SCAN_FEATURES: ["cardinality", "width", "children_card"] (Phase 2)
    - JOIN_FEATURES: ["cardinality", "width", "children_card"] (Phase 2)
    - FILTER_FEATURES: ["operator", "literal_feature"] (Phase 2)
    - COLUMN_FEATURES: ["avg_width", "correlation", "data_type", "n_distinct", "null_frac"] (Phase 2)
    - TABLE_FEATURES: ["reltuples", "relcols"] (Phase 2)

    Current implementation (Phase 1):
    - Only ENCODE_FEATURES (num_tables, num_joins)

    Usage:
        builder = FeatureBuilder()
        features = builder.extract_encode_features(parsed_query)
    """

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize feature builder

        Args:
            config: Optional configuration
        """
        self.config = config or {}

    def extract_encode_features(self, parsed_query: ParsedQuery) -> List[float]:
        """
        Extract ENCODE features (top-level query features)

        ENCODE_FEATURES = ["num_tables", "num_joins"]

        Args:
            parsed_query: Parsed query object

        Returns:
            List of 2 feature values
        """
        return [
            float(parsed_query.num_tables),
            float(parsed_query.num_joins),
        ]

    def extract_scan_features(self, parsed_query: ParsedQuery) -> List[List[float]]:
        """
        Extract SCAN features for each scan node (Phase 2)

        SCAN_FEATURES = ["cardinality", "width", "children_card"]

        Args:
            parsed_query: Parsed query object

        Returns:
            List of feature vectors, one per scan node
        """
        # TODO: Implement in Phase 2
        raise NotImplementedError("SCAN feature extraction not yet implemented")

    def extract_join_features(self, parsed_query: ParsedQuery) -> List[List[float]]:
        """
        Extract JOIN features for each join node (Phase 2)

        JOIN_FEATURES = ["cardinality", "width", "children_card"]

        Args:
            parsed_query: Parsed query object

        Returns:
            List of feature vectors, one per join node
        """
        # TODO: Implement in Phase 2
        raise NotImplementedError("JOIN feature extraction not yet implemented")

    def extract_filter_features(self, parsed_query: ParsedQuery) -> List[List[float]]:
        """
        Extract FILTER features for each filter condition (Phase 2)

        FILTER_FEATURES = ["operator", "literal_feature"]

        Args:
            parsed_query: Parsed query object

        Returns:
            List of feature vectors, one per filter
        """
        # TODO: Implement in Phase 2
        raise NotImplementedError("FILTER feature extraction not yet implemented")

    def extract_column_features(
        self,
        parsed_query: ParsedQuery,
        database_stats: Dict[str, Any]
    ) -> List[List[float]]:
        """
        Extract COLUMN features from database statistics (Phase 2)

        COLUMN_FEATURES = ["avg_width", "correlation", "data_type", "n_distinct", "null_frac"]

        Args:
            parsed_query: Parsed query object
            database_stats: Database statistics dictionary

        Returns:
            List of feature vectors, one per column
        """
        # TODO: Implement in Phase 2
        raise NotImplementedError("COLUMN feature extraction not yet implemented")

    def extract_table_features(
        self,
        parsed_query: ParsedQuery,
        database_stats: Dict[str, Any]
    ) -> List[List[float]]:
        """
        Extract TABLE features from database statistics (Phase 2)

        TABLE_FEATURES = ["reltuples", "relcols"]

        Args:
            parsed_query: Parsed query object
            database_stats: Database statistics dictionary

        Returns:
            List of feature vectors, one per table
        """
        # TODO: Implement in Phase 2
        raise NotImplementedError("TABLE feature extraction not yet implemented")

    def build_feature_vector(
        self,
        parsed_query: ParsedQuery,
        database_stats: Optional[Dict[str, Any]] = None
    ) -> List[float]:
        """
        Build complete feature vector for a query

        Current implementation (Phase 1):
        - Only includes ENCODE_FEATURES (num_tables, num_joins)

        Future implementation (Phase 2):
        - Will include all feature types and build graph structure

        Args:
            parsed_query: Parsed query object
            database_stats: Optional database statistics

        Returns:
            Feature vector
        """
        # Phase 1: Only ENCODE features
        encode_features = self.extract_encode_features(parsed_query)

        # In Phase 2, we would also extract:
        # - SCAN features
        # - JOIN features
        # - FILTER features
        # - COLUMN features (requires database_stats)
        # - TABLE features (requires database_stats)

        return encode_features

    def get_feature_names(self) -> List[str]:
        """
        Get feature names for interpretability

        Returns:
            List of feature names
        """
        # Phase 1: Only ENCODE features
        return [
            "num_tables",
            "num_joins",
        ]

        # Phase 2 will include:
        # - "scan_cardinality_{i}" for each scan node
        # - "join_cardinality_{i}" for each join node
        # - etc.

    def get_num_features(self) -> int:
        """
        Get number of features

        Returns:
            Number of features
        """
        return len(self.get_feature_names())

    # ========== Phase 2: DuckDB Plan Feature Extraction ==========

    def extract_scan_features_from_plan(
        self,
        parsed_plan: 'ParsedPlan'
    ) -> List[List[float]]:
        """
        Extract SCAN features from DuckDB plan nodes

        SCAN_FEATURES = ["cardinality", "est_width", "children_card"]

        Note: Using estimated cardinality from EXPLAIN plan.
        If you have EXPLAIN ANALYZE, use actual_cardinality instead.

        Args:
            parsed_plan: Parsed DuckDB plan

        Returns:
            List of feature vectors, one per scan node
            Each vector: [cardinality, width, children_card]
        """
        scan_features = []

        for scan_node in parsed_plan.scan_nodes:
            # Cardinality: use actual if available, otherwise estimated
            if scan_node.actual_cardinality is not None:
                cardinality = float(scan_node.actual_cardinality)
            else:
                cardinality = float(scan_node.cardinality)

            # Width: estimate from projections if available
            # For simplicity, use number of projections as proxy for width
            projections = scan_node.extra_info.get('Projections', [])
            est_width = float(len(projections)) if projections else 1.0

            # Children cardinality: scans are leaf nodes, so 0
            children_card = 0.0

            scan_features.append([cardinality, est_width, children_card])

        return scan_features

    def extract_join_features_from_plan(
        self,
        parsed_plan: 'ParsedPlan'
    ) -> List[List[float]]:
        """
        Extract JOIN features from DuckDB plan nodes

        JOIN_FEATURES = ["cardinality", "est_width", "children_card"]

        Args:
            parsed_plan: Parsed DuckDB plan

        Returns:
            List of feature vectors, one per join node
            Each vector: [cardinality, width, children_card]
        """
        join_features = []

        for join_node in parsed_plan.join_nodes:
            # Cardinality: use actual if available, otherwise estimated
            if join_node.actual_cardinality is not None:
                cardinality = float(join_node.actual_cardinality)
            else:
                cardinality = float(join_node.cardinality)

            # Width: estimate from number of output columns
            # For simplicity, use cardinality as proxy for width
            # In real implementation, should calculate from schema
            est_width = max(1.0, cardinality / 1000.0)

            # Children cardinality: sum of children's cardinalities
            children_card = 0.0
            for child in join_node.children:
                if child.actual_cardinality is not None:
                    children_card += float(child.actual_cardinality)
                else:
                    children_card += float(child.cardinality)

            join_features.append([cardinality, est_width, children_card])

        return join_features

    def build_feature_vector_from_plan(
        self,
        parsed_plan: 'ParsedPlan',
        include_scan_features: bool = True,
        include_join_features: bool = True
    ) -> List[float]:
        """
        Build complete feature vector from DuckDB plan

        This combines:
        - ENCODE features (num_tables, num_joins)
        - SCAN features (for each scan node)
        - JOIN features (for each join node)

        Args:
            parsed_plan: Parsed DuckDB plan
            include_scan_features: Whether to include scan features
            include_join_features: Whether to include join features

        Returns:
            Flattened feature vector
        """
        features = []

        # ENCODE features
        num_tables = len(parsed_plan.tables)
        num_joins = len(parsed_plan.join_nodes)
        features.extend([float(num_tables), float(num_joins)])

        # SCAN features
        if include_scan_features:
            scan_features = self.extract_scan_features_from_plan(parsed_plan)
            for scan_feat in scan_features:
                features.extend(scan_feat)

        # JOIN features
        if include_join_features:
            join_features = self.extract_join_features_from_plan(parsed_plan)
            for join_feat in join_features:
                features.extend(join_feat)

        return features

    def get_feature_names_from_plan(
        self,
        parsed_plan: 'ParsedPlan',
        include_scan_features: bool = True,
        include_join_features: bool = True
    ) -> List[str]:
        """
        Get feature names for plan-based features

        Args:
            parsed_plan: Parsed DuckDB plan
            include_scan_features: Whether to include scan features
            include_join_features: Whether to include join features

        Returns:
            List of feature names
        """
        names = []

        # ENCODE features
        names.extend(["num_tables", "num_joins"])

        # SCAN features
        if include_scan_features:
            for i, scan_node in enumerate(parsed_plan.scan_nodes):
                table_name = scan_node.extra_info.get('Table', f'scan_{i}')
                names.extend([
                    f"scan_{table_name}_cardinality",
                    f"scan_{table_name}_width",
                    f"scan_{table_name}_children_card"
                ])

        # JOIN features
        if include_join_features:
            for i in range(len(parsed_plan.join_nodes)):
                names.extend([
                    f"join_{i}_cardinality",
                    f"join_{i}_width",
                    f"join_{i}_children_card"
                ])

        return names
