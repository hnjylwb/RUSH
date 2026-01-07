import numpy as np
import pickle
import os
from typing import Dict, List
from .pipeline_encoder import PipelineEncoder

class AthenaTimePredictor:
    """
    Predicts Athena execution time based on query-level features and data scan volume
    
    This is different from pipeline-level resource prediction in that:
    1. It encodes the entire query (all pipelines combined) instead of individual pipelines
    2. It only predicts execution time, not other resources
    3. It includes estimated data scan volume as an additional feature
    """
    
    def __init__(self, model_path: str = None):
        """
        Initialize Athena time predictor
        
        Args:
            model_path: Path to trained model file (optional)
        """
        self.model_path = model_path or os.path.join(os.path.dirname(__file__), 'models', 'athena_time_model.pkl')
        self.pipeline_encoder = PipelineEncoder()  # Reuse existing encoder
        self.model = self._load_model()
        
        # Feature dimension: pipeline encoder features + 1 (data scan volume)
        self.feature_dim = self.pipeline_encoder.feature_dim + 1
    
    def _load_model(self):
        """Load trained Athena time prediction model"""
        try:
            if os.path.exists(self.model_path):
                with open(self.model_path, 'rb') as f:
                    return pickle.load(f)
            else:
                print(f"Athena time model not found at {self.model_path}, using dummy model")
                return DummyAthenaTimeModel()
        except Exception as e:
            print(f"Failed to load Athena time model: {e}")
            return DummyAthenaTimeModel()
    
    def predict_execution_time(self, pipeline_data: List[Dict], estimated_data_scan_cost: float) -> float:
        """
        Predict Athena execution time for a query
        
        Args:
            pipeline_data: List of pipeline dictionaries from query parsing
            estimated_data_scan_cost: Estimated cost of data scanning (proportional to data volume)
            
        Returns:
            Predicted execution time in seconds
        """
        # Step 1: Encode entire query (all pipelines combined)
        query_features = self._encode_query(pipeline_data)
        
        # Step 2: Add data scan volume feature
        # Normalize estimated cost to a reasonable scale for ML model
        # Athena cost is typically $5/TB, so cost of $0.001 = ~0.2GB
        data_scan_gb = estimated_data_scan_cost / 0.000005  # Convert cost to GB estimate
        
        # Combine query features with data scan volume
        full_features = np.append(query_features, data_scan_gb)
        
        # Step 3: Predict execution time
        predicted_time = self.model.predict(full_features.reshape(1, -1))[0]
        
        return max(predicted_time, 0.5)  # Minimum 0.5 seconds
    
    def _encode_query(self, pipeline_data: List[Dict]) -> np.ndarray:
        """
        Encode entire query by aggregating all pipeline features
        
        Args:
            pipeline_data: List of pipeline dictionaries
            
        Returns:
            Query-level feature vector
        """
        if not pipeline_data:
            return np.zeros(self.pipeline_encoder.feature_dim)
        
        # Initialize aggregated features
        aggregated_features = np.zeros(self.pipeline_encoder.feature_dim)
        
        # Process each pipeline and aggregate features
        for pipeline in pipeline_data:
            pipeline_features = self.pipeline_encoder.encode_pipeline(pipeline)
            aggregated_features += pipeline_features
        
        return aggregated_features
    
    def get_feature_names(self) -> List[str]:
        """
        Get human-readable feature names for debugging
        
        Returns:
            List of feature names including data scan volume
        """
        pipeline_feature_names = self.pipeline_encoder.get_feature_names()
        return pipeline_feature_names + ['data_scan_volume_gb']


class DummyAthenaTimeModel:
    """
    Dummy model for Athena time prediction when no trained model is available
    
    Uses heuristic-based estimation similar to the dummy XGBoost model
    """
    
    def predict(self, features: np.ndarray) -> np.ndarray:
        """
        Predict execution time using heuristics
        
        Args:
            features: Feature matrix (n_samples, n_features)
            
        Returns:
            Predicted execution times
        """
        predictions = []
        
        for feature_row in features:
            # Last feature is data scan volume in GB
            data_scan_gb = feature_row[-1]
            pipeline_features = feature_row[:-1]
            
            # Base time estimation from pipeline complexity
            # Sum of all non-zero features as complexity indicator
            complexity_score = np.sum(np.abs(pipeline_features[pipeline_features != 0]))
            
            # Base time from complexity (more conservative)
            base_time = 0.2 + complexity_score * 0.02
            
            # Data scan time: Athena is quite fast for small datasets
            # Adjust scan speeds to be more realistic for small data volumes
            if complexity_score > 15:
                scan_speed_gb_per_sec = 0.5  # 500 MB/s for very complex queries
            elif complexity_score > 8:
                scan_speed_gb_per_sec = 1.0  # 1 GB/s for medium-complex queries
            else:
                scan_speed_gb_per_sec = 2.0  # 2 GB/s for simple queries
            
            scan_time = data_scan_gb / scan_speed_gb_per_sec if scan_speed_gb_per_sec > 0 else 0
            
            # Total time: base processing time + data scan time
            # Athena overhead is reasonable for small queries
            startup_overhead = 0.5
            total_time = startup_overhead + base_time + scan_time
            
            predictions.append(total_time)
        
        return np.array(predictions)