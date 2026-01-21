"""
Simple ML Model Training for QaaS Cost Estimation

This script trains a simple regression model (Random Forest or Gradient Boosting)
to predict QaaS query runtime using extracted features from DuckDB plans.

Training Pipeline:
1. Load training data (queries + features + actual runtimes)
2. Extract features using PlanParser and FeatureBuilder
3. Train sklearn model (RandomForest or GradientBoosting)
4. Evaluate on test set
5. Save trained model

Usage:
    python tools/train_model.py --data training_data.json --output model.pkl

Training Data Format (JSON):
{
    "queries": [
        {
            "sql": "SELECT...",
            "qaas_runtime": 1.5,  # actual runtime on QaaS in seconds
            "data_scanned_mb": 100.0
        },
        ...
    ]
}
"""

import argparse
import json
import pickle
import duckdb
import sys
from pathlib import Path
from typing import List, Dict, Any, Tuple
import numpy as np

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
    from sklearn.linear_model import LinearRegression
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
except ImportError:
    print("Error: scikit-learn is required for training.")
    print("Install with: pip install scikit-learn")
    exit(1)

from ease.ml import PlanParser, FeatureBuilder


def compute_q_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Compute Q-error (quantile error) - common metric in query optimization

    Q-error = max(predicted/actual, actual/predicted)

    Args:
        y_true: Actual values
        y_pred: Predicted values

    Returns:
        Mean Q-error
    """
    # Avoid division by zero
    y_true = np.maximum(y_true, 1e-6)
    y_pred = np.maximum(y_pred, 1e-6)

    q_errors = np.maximum(y_pred / y_true, y_true / y_pred)
    return np.mean(q_errors)


def load_training_data(data_path: str) -> List[Dict[str, Any]]:
    """
    Load training data from JSON file

    Args:
        data_path: Path to training data JSON file

    Returns:
        List of query dictionaries
    """
    with open(data_path, 'r') as f:
        data = json.load(f)

    return data['queries']


def extract_features_for_query(
    sql: str,
    data_scanned_mb: float,
    db_path: str = ':memory:',
    use_plan_features: bool = True
) -> Tuple[List[float], List[str]]:
    """
    Extract features for a single query with fixed-length vector

    Uses aggregated features (sum, max, avg) for scans and joins to ensure
    fixed-length vectors regardless of query complexity.

    Args:
        sql: SQL query string
        data_scanned_mb: Data scanned in MB (from ResourceRequirements)
        db_path: Path to DuckDB database for EXPLAIN
        use_plan_features: Whether to use plan features (Phase 2)

    Returns:
        Tuple of (feature vector, feature names)
    """
    features = [data_scanned_mb]
    feature_names = ['data_scanned_mb']

    if use_plan_features:
        try:
            # Get DuckDB EXPLAIN plan
            conn = duckdb.connect(db_path)
            result = conn.execute(f'EXPLAIN (FORMAT JSON) {sql}').fetchone()

            if result:
                plan_json = json.loads(result[1])

                # Parse plan
                plan_parser = PlanParser()
                parsed_plan = plan_parser.parse_plan(plan_json, sql)

                # Extract ENCODE features
                num_tables = float(len(parsed_plan.tables))
                num_joins = float(len(parsed_plan.join_nodes))
                features.extend([num_tables, num_joins])
                feature_names.extend(['num_tables', 'num_joins'])

                # Extract and aggregate SCAN features
                feature_builder = FeatureBuilder()
                scan_features = feature_builder.extract_scan_features_from_plan(parsed_plan)

                if scan_features:
                    # Aggregate: sum, max, avg of [cardinality, width, children_card]
                    scan_array = np.array(scan_features)
                    scan_sum = scan_array.sum(axis=0)
                    scan_max = scan_array.max(axis=0)
                    scan_avg = scan_array.mean(axis=0)

                    features.extend([
                        scan_sum[0], scan_sum[1], scan_sum[2],  # sum_card, sum_width, sum_children
                        scan_max[0], scan_max[1], scan_max[2],  # max_card, max_width, max_children
                        scan_avg[0], scan_avg[1], scan_avg[2],  # avg_card, avg_width, avg_children
                    ])
                    feature_names.extend([
                        'scan_sum_cardinality', 'scan_sum_width', 'scan_sum_children_card',
                        'scan_max_cardinality', 'scan_max_width', 'scan_max_children_card',
                        'scan_avg_cardinality', 'scan_avg_width', 'scan_avg_children_card',
                    ])
                else:
                    # No scans - add zeros
                    features.extend([0.0] * 9)
                    feature_names.extend([
                        'scan_sum_cardinality', 'scan_sum_width', 'scan_sum_children_card',
                        'scan_max_cardinality', 'scan_max_width', 'scan_max_children_card',
                        'scan_avg_cardinality', 'scan_avg_width', 'scan_avg_children_card',
                    ])

                # Extract and aggregate JOIN features
                join_features = feature_builder.extract_join_features_from_plan(parsed_plan)

                if join_features:
                    # Aggregate: sum, max, avg of [cardinality, width, children_card]
                    join_array = np.array(join_features)
                    join_sum = join_array.sum(axis=0)
                    join_max = join_array.max(axis=0)
                    join_avg = join_array.mean(axis=0)

                    features.extend([
                        join_sum[0], join_sum[1], join_sum[2],
                        join_max[0], join_max[1], join_max[2],
                        join_avg[0], join_avg[1], join_avg[2],
                    ])
                    feature_names.extend([
                        'join_sum_cardinality', 'join_sum_width', 'join_sum_children_card',
                        'join_max_cardinality', 'join_max_width', 'join_max_children_card',
                        'join_avg_cardinality', 'join_avg_width', 'join_avg_children_card',
                    ])
                else:
                    # No joins - add zeros
                    features.extend([0.0] * 9)
                    feature_names.extend([
                        'join_sum_cardinality', 'join_sum_width', 'join_sum_children_card',
                        'join_max_cardinality', 'join_max_width', 'join_max_children_card',
                        'join_avg_cardinality', 'join_avg_width', 'join_avg_children_card',
                    ])

            conn.close()
        except Exception as e:
            print(f"Warning: Failed to extract plan features: {e}")
            print("Falling back to data_scanned only")

    return features, feature_names


def prepare_dataset(
    queries: List[Dict[str, Any]],
    db_path: str = ':memory:',
    use_plan_features: bool = True
) -> Tuple[np.ndarray, np.ndarray, List[str]]:
    """
    Prepare dataset for training

    Args:
        queries: List of query dictionaries
        db_path: Path to DuckDB database
        use_plan_features: Whether to use plan features

    Returns:
        Tuple of (X, y, feature_names)
    """
    X_list = []
    y_list = []
    feature_names = None

    print(f"Extracting features for {len(queries)} queries...")

    for i, query in enumerate(queries):
        if (i + 1) % 10 == 0:
            print(f"  Processed {i + 1}/{len(queries)} queries")

        sql = query['sql']
        qaas_runtime = query['qaas_runtime']
        data_scanned_mb = query['data_scanned_mb']

        # Extract features
        features, names = extract_features_for_query(
            sql, data_scanned_mb, db_path, use_plan_features
        )

        if feature_names is None:
            feature_names = names

        X_list.append(features)
        y_list.append(qaas_runtime)

    X = np.array(X_list)
    y = np.array(y_list)

    print(f"Dataset prepared: {X.shape[0]} samples, {X.shape[1]} features")

    return X, y, feature_names


def train_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    model_type: str = 'random_forest'
):
    """
    Train a regression model

    Args:
        X_train: Training features
        y_train: Training labels
        model_type: Type of model ('linear', 'random_forest', 'gradient_boosting')

    Returns:
        Trained model
    """
    print(f"\nTraining {model_type} model...")

    if model_type == 'linear':
        model = LinearRegression()
    elif model_type == 'random_forest':
        model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            min_samples_split=5,
            random_state=42,
            n_jobs=-1
        )
    elif model_type == 'gradient_boosting':
        model = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            random_state=42
        )
    else:
        raise ValueError(f"Unknown model type: {model_type}")

    model.fit(X_train, y_train)

    print(f"Training completed!")

    return model


def evaluate_model(
    model,
    X_test: np.ndarray,
    y_test: np.ndarray,
    feature_names: List[str]
):
    """
    Evaluate model performance

    Args:
        model: Trained model
        X_test: Test features
        y_test: Test labels
        feature_names: List of feature names
    """
    print("\n" + "="*60)
    print("Model Evaluation")
    print("="*60)

    # Make predictions
    y_pred = model.predict(X_test)

    # Compute metrics
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    mae = mean_absolute_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    q_error = compute_q_error(y_test, y_pred)

    print(f"\nPerformance Metrics:")
    print(f"  MSE:      {mse:.4f}")
    print(f"  RMSE:     {rmse:.4f}")
    print(f"  MAE:      {mae:.4f}")
    print(f"  R²:       {r2:.4f}")
    print(f"  Q-error:  {q_error:.4f}")

    # Feature importance (if available)
    if hasattr(model, 'feature_importances_'):
        print(f"\nTop 10 Feature Importances:")
        importances = model.feature_importances_
        indices = np.argsort(importances)[::-1][:10]

        for i, idx in enumerate(indices):
            print(f"  {i+1}. {feature_names[idx]}: {importances[idx]:.4f}")

    # Show some predictions
    print(f"\nSample Predictions (first 10):")
    print(f"  {'Actual':<12} {'Predicted':<12} {'Error':<12} {'Q-error':<12}")
    print(f"  {'-'*12} {'-'*12} {'-'*12} {'-'*12}")

    for i in range(min(10, len(y_test))):
        actual = y_test[i]
        pred = y_pred[i]
        error = pred - actual
        q_err = max(pred/max(actual, 1e-6), actual/max(pred, 1e-6))
        print(f"  {actual:<12.4f} {pred:<12.4f} {error:<12.4f} {q_err:<12.4f}")


def save_model(model, feature_names: List[str], output_path: str):
    """
    Save trained model and metadata

    Args:
        model: Trained model
        feature_names: List of feature names
        output_path: Output path for model file
    """
    model_data = {
        'model': model,
        'feature_names': feature_names,
        'model_type': type(model).__name__
    }

    with open(output_path, 'wb') as f:
        pickle.dump(model_data, f)

    print(f"\nModel saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Train simple ML model for QaaS cost estimation')
    parser.add_argument('--data', type=str, required=True, help='Path to training data JSON')
    parser.add_argument('--output', type=str, default='qaas_cost_model.pkl', help='Output path for trained model')
    parser.add_argument('--model', type=str, default='random_forest',
                       choices=['linear', 'random_forest', 'gradient_boosting'],
                       help='Model type')
    parser.add_argument('--db', type=str, default=':memory:', help='Path to DuckDB database')
    parser.add_argument('--no-plan-features', action='store_true', help='Only use data_scanned feature')
    parser.add_argument('--test-split', type=float, default=0.2, help='Test set split ratio')

    args = parser.parse_args()

    print("="*60)
    print("QaaS Cost Model Training")
    print("="*60)

    # Load data
    print(f"\nLoading training data from: {args.data}")
    queries = load_training_data(args.data)
    print(f"Loaded {len(queries)} queries")

    # Prepare dataset
    use_plan_features = not args.no_plan_features
    X, y, feature_names = prepare_dataset(queries, args.db, use_plan_features)

    # Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_split, random_state=42
    )

    print(f"\nDataset split:")
    print(f"  Training set: {len(X_train)} samples")
    print(f"  Test set:     {len(X_test)} samples")

    # Train model
    model = train_model(X_train, y_train, args.model)

    # Evaluate model
    evaluate_model(model, X_test, y_test, feature_names)

    # Save model
    save_model(model, feature_names, args.output)

    print("\n" + "="*60)
    print("Training completed successfully!")
    print("="*60)


if __name__ == '__main__':
    main()
