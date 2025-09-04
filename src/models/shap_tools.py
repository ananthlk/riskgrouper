# shap_tools.py
# SHAP explainer creation, caching, and local/global factor extraction

import shap
import pandas as pd
import numpy as np
import os
import json
import joblib
from typing import Dict, Any

def get_shap_explainer(model, X_bg: pd.DataFrame, model_type: str, cache_path: str, logger, cfg: Dict[str, Any]) -> shap.Explainer:
    """
    Create or load a SHAP explainer with strict config enforcement.
    Enforces train sample cap for background data.
    """
    sample_cap = cfg.get('EXPLAIN_TRAIN_SAMPLE_SIZE', 1000)
    if X_bg.shape[0] > sample_cap:
        X_bg = X_bg.sample(n=sample_cap, random_state=cfg.get('SEED', 42))
    if os.path.exists(cache_path):
        logger.info(f"Loading SHAP explainer from cache: {cache_path}")
        explainer = joblib.load(cache_path)
    else:
        logger.info(f"Building SHAP explainer and caching to: {cache_path}")
        explainer = shap.Explainer(
            model,
            X_bg,
            model_output="probability"
        )
        joblib.dump(explainer, cache_path)
    return explainer

def extract_local_factors(
    explainer: shap.Explainer,
    X: pd.DataFrame,
    cfg: Dict[str, Any],
    binary_features: set
) -> pd.DataFrame:
    """
    Extract local SHAP factors for each individual.
    Output schema:
        row_idx, base_value, increasing_factors, decreasing_factors, reconstruction_error
    """
    epsilon = cfg.get('EPSILON', 1e-6)
    topn = cfg.get('TOP_N', 25)
    local_results = []
    for idx, row in X.iterrows():
        result = {
            "row_idx": idx,
            "base_value": None,
            "increasing_factors": [],
            "decreasing_factors": [],
            "reconstruction_error": None
        }
        try:
            if row.isnull().all():
                local_results.append(result)
                continue
            sv = explainer(row.values.reshape(1, -1))
            contribs = sv.values[0]
            base = sv.base_values[0]
            if any(c is None for c in contribs):
                local_results.append(result)
                continue
            increasing = []
            decreasing = []
            for i, (feat, val, contrib) in enumerate(zip(X.columns, row.values, contribs)):
                try:
                    contrib_val = float(contrib)
                except Exception:
                    continue
                if feat in binary_features:
                    if val == 1:
                        if contrib_val > 0:
                            increasing.append((feat, contrib_val))
                        elif contrib_val < 0:
                            decreasing.append((feat, contrib_val))
                else:
                    if abs(contrib_val) >= epsilon:
                        if contrib_val > 0:
                            increasing.append((feat, contrib_val))
                        elif contrib_val < 0:
                            decreasing.append((feat, contrib_val))
            increasing = sorted(increasing, key=lambda x: -x[1])[:topn]
            decreasing = sorted(decreasing, key=lambda x: x[1])[:topn]
            result.update({
                "base_value": float(base),
                "increasing_factors": increasing,
                "decreasing_factors": decreasing,
                "reconstruction_error": abs(base + sum([c for _, c in increasing + decreasing]) - sv.data[0])
            })
        except Exception:
            pass
        local_results.append(result)
    return pd.DataFrame(local_results)

def extract_global_factors(
    explainer: shap.Explainer,
    X: pd.DataFrame,
    cfg: Dict[str, Any]
) -> pd.DataFrame:
    """
    Extract global SHAP factors (mean absolute SHAP value per feature).
    Output schema:
        feature, mean_abs_shap, rank
    """
    epsilon = cfg.get('EPSILON', 1e-6)
    topn = cfg.get('TOP_N', 25)
    sv = explainer(X)
    mean_abs_shap = np.abs(sv.values).mean(axis=0)
    features = X.columns
    global_results = [
        {"feature": feat, "mean_abs_shap": float(val)}
        for feat, val in zip(features, mean_abs_shap)
        if abs(val) >= epsilon
    ]
    global_results = sorted(global_results, key=lambda x: -x["mean_abs_shap"])[:topn]
    for i, res in enumerate(global_results):
        res["rank"] = i + 1
    return pd.DataFrame(global_results)
