"""
Logistic Regression model training module.
Implements train_logistic for modular pipeline orchestration.
"""
import pandas as pd
from sklearn.linear_model import LogisticRegression

def train_logistic(train_data: pd.DataFrame, features: list, config) -> LogisticRegression:
    """
    Train a Logistic Regression model on the provided data and features.
    Args:
        train_data: Training data DataFrame
        features: List of feature names
        config: Config object with hyperparameters
    Returns:
        Trained LogisticRegression model
    """
    import numpy as np
    from sklearn.utils import check_random_state

    # Set random seed for reproducibility
    seed = getattr(config, 'seed', 42)
    np.random.seed(seed)

    # Extract hyperparameters from config or use defaults
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    # Define pipeline with scaling and logistic regression
    pipe = Pipeline(steps=[
        ("scale", StandardScaler(with_mean=False)),
        ("logreg", LogisticRegression(
            solver="saga",
            penalty="l2",
            class_weight="balanced",
            C=1.0,
            max_iter=2000,
            tol=1e-4,
            random_state=42,
            verbose=0
        ))
    ])

    X = train_data[features].fillna(0)
    y = train_data[config['TARGET']]

    pipe.fit(X, y)

    # Extract and print/log coefficients
    import logging
    coef = pipe.named_steps['logreg'].coef_[0]
    intercept = pipe.named_steps['logreg'].intercept_[0]
    coef_dict = dict(zip(features, coef))
    logging.info("Logistic Regression coefficients:")
    for fname, val in coef_dict.items():
        logging.info(f"  {fname}: {val:.6f}")
    logging.info(f"Intercept: {intercept:.6f}")
    print("Logistic Regression coefficients:")
    for fname, val in coef_dict.items():
        print(f"  {fname}: {val:.6f}")
    print(f"Intercept: {intercept:.6f}")
    return pipe
