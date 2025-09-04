# metrics.py
# Metric calculation and logging for Risk Grouper Refactor

from typing import Dict, Any
import numpy as np
import pandas as pd
import json
import os
from sklearn.metrics import roc_auc_score, average_precision_score, f1_score, precision_score, recall_score, accuracy_score, confusion_matrix, log_loss

def compute_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_prob: np.ndarray,
    threshold: float,
    model_name: str,
    split_name: str,
    output_dir: str,
    extra: Dict[str, Any] = None
) -> Dict[str, Any]:
    metrics = {}
    metrics['roc_auc'] = float(roc_auc_score(y_true, y_prob)) if len(np.unique(y_true)) > 1 else None
    metrics['pr_auc'] = float(average_precision_score(y_true, y_prob)) if len(np.unique(y_true)) > 1 else None
    metrics['f1'] = float(f1_score(y_true, y_pred))
    metrics['precision'] = float(precision_score(y_true, y_pred))
    metrics['recall'] = float(recall_score(y_true, y_pred))
    metrics['accuracy'] = float(accuracy_score(y_true, y_pred))
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel() if len(np.unique(y_true)) > 1 else (0,0,0,0)
    metrics['tp'] = int(tp)
    metrics['fp'] = int(fp)
    metrics['tn'] = int(tn)
    metrics['fn'] = int(fn)
    metrics['threshold_used'] = float(threshold)
    # Pseudo-R² for logistic
    if model_name == 'logistic':
        try:
            ll_model = -log_loss(y_true, y_prob, normalize=False)
            p_null = np.mean(y_true)
            ll_null = -log_loss(y_true, np.full_like(y_true, p_null), normalize=False)
            metrics['pseudo_r2'] = float(1 - (ll_model / ll_null))
        except Exception:
            metrics['pseudo_r2'] = None
    else:
        metrics['pseudo_r2'] = "N/A"
    # Add extra fields
    if extra:
        metrics.update(extra)
    return metrics

def save_metrics_csv_json(metrics_rows, output_csv, output_json):
    df = pd.DataFrame(metrics_rows)
    df.to_csv(output_csv, index=False)
    with open(output_json, "w") as f:
        json.dump(metrics_rows, f, indent=2)
