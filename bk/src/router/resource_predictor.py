import numpy as np
import pickle
import os
from typing import Dict, Any

class ResourcePredictor:
    """
    Uses XGBoost model to predict resource usage (CPU, memory, IO, duration) from pipeline features
    """
    
    def __init__(self, model_path: str = None):
        self.model = None
        self.model_path = model_path or os.path.join(os.path.dirname(__file__), 'models', 'resource_model.pkl')
        self._load_model()
    
    def _load_model(self):
        """Load XGBoost model from file"""
        try:
            if os.path.exists(self.model_path):
                with open(self.model_path, 'rb') as f:
                    self.model = pickle.load(f)
                print(f"Loaded XGBoost model from {self.model_path}")
            else:
                print(f"Model file not found at {self.model_path}, using fallback predictions")
                self.model = None
        except Exception as e:
            print(f"Failed to load XGBoost model: {e}")
            self.model = None
    
    def predict_resources(self, features: np.ndarray) -> Dict[str, float]:
        """
        Predict resource usage for a pipeline
        
        Args:
            features: Feature vector from PipelineEncoder
            
        Returns:
            Dictionary with predicted CPU, memory, IO usage and duration
        """
        if self.model is not None:
            try:
                # Reshape features for prediction if needed
                if features.ndim == 1:
                    features = features.reshape(1, -1)
                
                # XGBoost prediction - assumes model outputs [cpu, memory, io, duration]
                predictions = self.model.predict(features)
                
                if predictions.ndim == 1:
                    # Single prediction
                    return {
                        'cpu_usage': float(predictions[0]) if len(predictions) > 0 else 0.5,
                        'memory_usage': float(predictions[1]) if len(predictions) > 1 else 0.5,
                        'io_usage': float(predictions[2]) if len(predictions) > 2 else 0.3,
                        'duration': float(predictions[3]) if len(predictions) > 3 else 2.0
                    }
                else:
                    # Multiple outputs
                    pred = predictions[0] if len(predictions) > 0 else [0.5, 0.5, 0.3, 2.0]
                    return {
                        'cpu_usage': float(pred[0]) if len(pred) > 0 else 0.5,
                        'memory_usage': float(pred[1]) if len(pred) > 1 else 0.5,
                        'io_usage': float(pred[2]) if len(pred) > 2 else 0.3,
                        'duration': float(pred[3]) if len(pred) > 3 else 2.0
                    }
                    
            except Exception as e:
                print(f"XGBoost prediction failed: {e}")
                return self._fallback_prediction(features)
        else:
            return self._fallback_prediction(features)
    
    def _fallback_prediction(self, features: np.ndarray) -> Dict[str, float]:
        """
        Fallback prediction when XGBoost model is not available
        Uses simple heuristics based on feature values
        """
        # Extract relevant features for heuristic prediction
        operator_count = features[0] if len(features) > 0 else 1
        total_cardinality = features[1] if len(features) > 1 else 0  # Already log-transformed
        
        # Heuristic predictions based on pipeline characteristics
        # CPU usage: higher for more operators and complex operations
        cpu_usage = min(0.1 + (operator_count * 0.1) + (total_cardinality * 0.05), 1.0)
        
        # Memory usage: higher for joins and aggregations
        has_join = any(features[30:33]) if len(features) > 32 else False
        has_agg = features[33] if len(features) > 33 else False
        memory_base = 0.2
        if has_join:
            memory_base += 0.3
        if has_agg:
            memory_base += 0.2
        memory_usage = min(memory_base + (total_cardinality * 0.03), 1.0)
        
        # IO usage: higher for scans and large cardinalities
        io_usage = min(0.1 + (total_cardinality * 0.04), 1.0)
        
        # Duration: based on complexity
        duration = max(0.5, operator_count * 0.5 + total_cardinality * 0.2)
        
        return {
            'cpu_usage': cpu_usage,
            'memory_usage': memory_usage,
            'io_usage': io_usage,
            'duration': duration
        }
    
    def predict_multiple_pipelines(self, pipeline_features: list) -> list:
        """
        Predict resources for multiple pipelines
        
        Args:
            pipeline_features: List of feature vectors
            
        Returns:
            List of prediction dictionaries
        """
        predictions = []
        for features in pipeline_features:
            pred = self.predict_resources(features)
            predictions.append(pred)
        
        return predictions
    
    def aggregate_pipeline_predictions(self, predictions: list) -> Dict[str, float]:
        """
        Aggregate predictions from multiple pipelines into overall query prediction
        
        Args:
            predictions: List of prediction dictionaries from multiple pipelines
            
        Returns:
            Aggregated prediction for entire query
        """
        if not predictions:
            return {'cpu_usage': 0.5, 'memory_usage': 0.5, 'io_usage': 0.3, 'duration': 2.0}
        
        # Aggregate strategy: sum for duration, max for resource usage
        total_duration = sum(pred['duration'] for pred in predictions)
        max_cpu = max(pred['cpu_usage'] for pred in predictions)
        max_memory = max(pred['memory_usage'] for pred in predictions)
        max_io = max(pred['io_usage'] for pred in predictions)
        
        return {
            'cpu_usage': max_cpu,
            'memory_usage': max_memory,
            'io_usage': max_io,
            'duration': total_duration
        }
    
    def save_model(self, model, path: str = None):
        """Save trained XGBoost model"""
        save_path = path or self.model_path
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        
        try:
            with open(save_path, 'wb') as f:
                pickle.dump(model, f)
            print(f"Model saved to {save_path}")
            self.model = model
        except Exception as e:
            print(f"Failed to save model: {e}")
    
    def create_dummy_model(self):
        """Create a dummy model for testing purposes"""
        try:
            # This would normally be replaced with actual XGBoost model training
            print("Creating dummy model for testing...")
            
            # Create a simple callable object that can be pickled
            import types
            
            def dummy_predict(X):
                # Return dummy predictions: [cpu, memory, io, duration]
                if X.ndim == 1:
                    X = X.reshape(1, -1)
                
                predictions = []
                for row in X:
                    # Simple heuristic based on first few features
                    cpu = min(0.2 + row[0] * 0.1, 1.0)  # Based on operator count
                    memory = min(0.3 + row[1] * 0.05, 1.0)  # Based on cardinality
                    io = min(0.2 + row[1] * 0.03, 0.8)
                    duration = max(1.0, row[0] * 0.8 + row[1] * 0.3)
                    predictions.append([cpu, memory, io, duration])
                
                return np.array(predictions)
            
            # Create dummy model object with predict method
            dummy_model = types.SimpleNamespace()
            dummy_model.predict = dummy_predict
            
            self.save_model(dummy_model)
            return dummy_model
            
        except Exception as e:
            print(f"Failed to create dummy model: {e}")
            return None