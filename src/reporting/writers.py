# Explicitly export all writer functions
__all__ = [
    'write_csv',
    'write_json',
    'save_confusion_matrix',
    'write_metrics',
    'write_confusion_matrix'
]
# writers.py
# CSV/JSON writers and confusion matrix image generation for Risk Grouper Refactor

import pandas as pd
import json
import os
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict

def write_csv(data: List[Dict], path: str):
    df = pd.DataFrame(data)
    df.to_csv(path, index=False)

def write_json(data: List[Dict], path: str):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def write_metrics(metrics_logistic, metrics_xgb, config):
    """
    Write metrics for all splits (train, test, validation) for both models to CSV and JSON files in output/metrics/.
    metrics structure for each model should be: {"train": {...}, "test": {...}, "validation": {...}}
    """
    os.makedirs('output/metrics', exist_ok=True)
    metrics = {
        'logistic': metrics_logistic,
        'xgboost': metrics_xgb
    }
    # Write JSON
    with open('output/metrics/metrics.json', 'w') as f:
        json.dump(metrics, f, indent=2)

    # Write CSV: flatten to rows with model, split, and metrics
    rows = []
    for model, splits in metrics.items():
        if splits is not None:
            for split_name, split_metrics in splits.items():
                if split_metrics is not None:
                    row = {'model': model, 'split': split_name}
                    row.update(split_metrics)
                    rows.append(row)
    pd.DataFrame(rows).to_csv('output/metrics/metrics.csv', index=False)

def write_confusion_matrix(metrics_logistic, metrics_xgb, config):
    """
    Write confusion matrices for both models to CSV and PNG files in output/metrics/.
    """
    import sys
    current_module = sys.modules[__name__]
    os.makedirs('output/metrics', exist_ok=True)
    # Assume metrics contain confusion_matrix and labels
    for model, metrics in [('logistic', metrics_logistic), ('xgboost', metrics_xgb)]:
        if metrics is not None and 'confusion_matrix' in metrics and 'labels' in metrics:
            cm = metrics['confusion_matrix']
            labels = metrics['labels']
            csv_path = f'output/metrics/confusion_matrix_{model}.csv'
            png_path = f'output/metrics/confusion_matrix_{model}.png'
            getattr(current_module, 'save_confusion_matrix')(cm, labels, csv_path, png_path)
