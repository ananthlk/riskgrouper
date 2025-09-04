"""
XGBoost model training module.
Implements train_xgboost for modular pipeline orchestration.
"""
import pandas as pd
import xgboost as xgb

def train_xgboost(train_data: pd.DataFrame, features: list, config) -> xgb.XGBClassifier:
    """
    Train an XGBoost model on the provided data and features.
    Args:
        train_data: Training data DataFrame
        features: List of feature names
        config: Config object with hyperparameters
    Returns:
        Trained XGBClassifier model
    """
    import numpy as np
    from sklearn.model_selection import RandomizedSearchCV
    from scipy.stats import randint, uniform

    # Set random seed for reproducibility
    seed = getattr(config, 'seed', 42)
    np.random.seed(seed)

    # Extract hyperparameters from config or use defaults
    xgb_params = getattr(config, 'xgboost_params', {})

    X = train_data[features]
    y = train_data[config['TARGET']]

    # Calculate scale_pos_weight for class imbalance using resampled data
    n_pos = (y == 1).sum()
    n_neg = (y == 0).sum()
    scale_pos_weight = n_neg / n_pos if n_pos > 0 else 1
    xgb_params['scale_pos_weight'] = scale_pos_weight
    print(f"XGBoost scale_pos_weight set to: {scale_pos_weight} (neg: {n_neg}, pos: {n_pos})")

    # Define parameter grid for RandomizedSearchCV
    param_dist = {
        'n_estimators': randint(50, 300),
        'max_depth': randint(3, 10),
        'learning_rate': uniform(0.01, 0.3),
        'subsample': uniform(0.6, 0.4),
        'colsample_bytree': uniform(0.6, 0.4),
        'gamma': uniform(0, 0.5),
        'reg_alpha': uniform(0, 1),
        'reg_lambda': uniform(0, 1),
        'scale_pos_weight': [scale_pos_weight],
    }

    base_model = xgb.XGBClassifier(random_state=seed, **{k: v for k, v in xgb_params.items() if k != 'scale_pos_weight'})
    search = RandomizedSearchCV(
        base_model,
        param_distributions=param_dist,
        n_iter=20,
        scoring='roc_auc',
        cv=3,
        verbose=1,
        n_jobs=-1,
        random_state=seed,
    )
    search.fit(X, y)
    print(f"Best XGBoost params: {search.best_params_}")
    return search.best_estimator_
