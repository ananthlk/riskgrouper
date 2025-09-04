# updated_seperate_regression_models.py
# Main entry point for Risk Grouper Refactor
# Implements CLI flags and orchestrates model pipelines

import argparse
import sys
import os
import logging
import platform
import json
import numpy as np
import random
from src.config import DEFAULT_CONFIG

# CLI flags and config loading

def parse_args():
    parser = argparse.ArgumentParser(description="Risk Grouper Refactor")
    parser.add_argument('--model', choices=['logistic', 'xgboost', 'both'], default=DEFAULT_CONFIG['MODEL'])
    parser.add_argument('--include-splits', choices=['test', 'val_test', 'train_val_test'], default=DEFAULT_CONFIG['INCLUDE_SPLITS'])
    parser.add_argument('--explain-train-sample-size', type=int, default=DEFAULT_CONFIG['EXPLAIN_TRAIN_SAMPLE_SIZE'])
    parser.add_argument('--epsilon', type=float, default=DEFAULT_CONFIG['EPSILON'])
    parser.add_argument('--topn', type=int, default=DEFAULT_CONFIG['TOP_N'])
    parser.add_argument('--seed', type=int, default=DEFAULT_CONFIG['SEED'])
    args = parser.parse_args()
    cfg = DEFAULT_CONFIG.copy()
    cfg.update(vars(args))
    return cfg

# Set seeds for reproducibility

def set_seeds(seed: int):
    np.random.seed(seed)
    random.seed(seed)
    try:
        import xgboost as xgb
        xgb.set_config(verbosity=0)
    except ImportError:
        pass

# Run metadata

def get_run_metadata(run_id: str) -> dict:
    metadata = {
        "run_id": run_id,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "packages": {},
        "git_commit": os.environ.get("GIT_COMMIT", "N/A"),
    }
    try:
        import sklearn, xgboost, pandas
        metadata["packages"] = {
            "sklearn": sklearn.__version__,
            "xgboost": xgboost.__version__,
            "pandas": pandas.__version__,
        }
    except Exception:
        pass
    return metadata

# Placeholder for main orchestration

def main():
    cfg = parse_args()
    set_seeds(cfg['SEED'])
    run_id = f"run_{np.random.randint(1e9)}"
    metadata = get_run_metadata(run_id)
    # Save run metadata
    metrics_dir = os.path.join("output", "metrics")
    os.makedirs(metrics_dir, exist_ok=True)
    with open(os.path.join(metrics_dir, "run_metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Run metadata saved to {os.path.join(metrics_dir, 'run_metadata.json')}")

    # --- Invoke pipeline orchestrator ---
    from src.orchestration.pipeline_orchestrator import PipelineOrchestrator
    orchestrator = PipelineOrchestrator(cfg)
    orchestrator.run()

if __name__ == "__main__":
    main()
