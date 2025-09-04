# individuals.py
# Consolidated per-individual output writer for Risk Grouper Refactor

import pandas as pd
import json
from typing import Dict, Any

def write_consolidated_individuals(
    df: pd.DataFrame,
    logistic_preds,
    xgb_preds,
    shap_logistic,
    shap_xgb,
    cfg: Dict[str, Any]
) -> None:
    """
    Writes consolidated per-individual predictions for specified date window and splits.
    Schema: fh_id, effective_month_start, actual_any_event_next_90d,
    logistic.prob, logistic.pred_label, logistic.threshold,
    logistic.increasing_factors, logistic.decreasing_factors,
    xgboost.prob, xgboost.pred_label, xgboost.threshold,
    xgboost.increasing_factors, xgboost.decreasing_factors, xgboost.base_value
    """
    # Implementation: Write per-member predictions and SHAP factors to CSV
    output_path = cfg.get('INDIVIDUAL_OUTPUT_PATH', 'output/individual_predictions.csv')
    # Align all arrays to the minimum length
    n = min(len(df), len(logistic_preds) if logistic_preds is not None else len(df), len(xgb_preds) if xgb_preds is not None else len(df),
            len(shap_logistic) if shap_logistic is not None else len(df), len(shap_xgb) if shap_xgb is not None else len(df))
    rows = []
    import logging
    logger = logging.getLogger("individuals_writer")
    for idx in range(n):
        row = df.iloc[idx]
        # Robust column access
        fh_id = row.get('fh_id', None)
        if fh_id is None:
            fh_id = row.get('FH_ID', None)
        if fh_id is None:
            logger.warning(f"Missing fh_id/FH_ID for row {idx}")
        effective_month_start = row.get('effective_month_start', None)
        if effective_month_start is None:
            effective_month_start = row.get('EFFECTIVE_MONTH_START', None)
        if effective_month_start is None:
            logger.warning(f"Missing effective_month_start/EFFECTIVE_MONTH_START for row {idx}")
        actual_event = row.get('actual_any_event_next_90d', None)
        if actual_event is None:
            actual_event = row.get('ANY_EVENT_NEXT_90D', None)
        if actual_event is None:
            logger.warning(f"Missing actual_any_event_next_90d/ANY_EVENT_NEXT_90D for row {idx}")
        # Logistic model outputs
        logistic_prob = logistic_preds[idx] if logistic_preds is not None and idx < len(logistic_preds) else None
        logistic_pred_label = int(logistic_prob >= cfg.get('LOGISTIC_THRESHOLD', 0.5)) if logistic_prob is not None else None
        logistic_increasing = shap_logistic.iloc[idx]['increasing_factors'] if shap_logistic is not None and idx < len(shap_logistic) else None
        logistic_decreasing = shap_logistic.iloc[idx]['decreasing_factors'] if shap_logistic is not None and idx < len(shap_logistic) else None
        # XGBoost model outputs
        xgb_prob = xgb_preds[idx] if xgb_preds is not None and idx < len(xgb_preds) else None
        xgb_pred_label = int(xgb_prob >= cfg.get('XGB_THRESHOLD', 0.5)) if xgb_prob is not None else None
        xgb_increasing = shap_xgb.iloc[idx]['increasing_factors'] if shap_xgb is not None and idx < len(shap_xgb) else None
        xgb_decreasing = shap_xgb.iloc[idx]['decreasing_factors'] if shap_xgb is not None and idx < len(shap_xgb) else None
        xgb_base_value = shap_xgb.iloc[idx]['base_value'] if shap_xgb is not None and idx < len(shap_xgb) else None
        rows.append({
            'fh_id': fh_id,
            'effective_month_start': effective_month_start,
            'actual_any_event_next_90d': actual_event,
            'logistic.prob': logistic_prob,
            'logistic.pred_label': logistic_pred_label,
            'logistic.increasing_factors': json.dumps(logistic_increasing) if logistic_increasing is not None else None,
            'logistic.decreasing_factors': json.dumps(logistic_decreasing) if logistic_decreasing is not None else None,
            'xgboost.prob': xgb_prob,
            'xgboost.pred_label': xgb_pred_label,
            'xgboost.increasing_factors': json.dumps(xgb_increasing) if xgb_increasing is not None else None,
            'xgboost.decreasing_factors': json.dumps(xgb_decreasing) if xgb_decreasing is not None else None,
            'xgboost.base_value': xgb_base_value
        })
    out_df = pd.DataFrame(rows)
    out_df.to_csv(output_path, index=False)
    print(f"Individual predictions and SHAP factors written to {output_path}")
    
def write_full_consolidated_individuals(
    splits: dict,
    logistic_preds_dict: dict,
    xgb_preds_dict: dict,
    shap_xgb_dict: dict,
    logistic_coeffs: dict,
    cfg: Dict[str, Any]
) -> None:
    """
    Consolidate all individuals from train, test, validate splits with effective_dates >= '2023-01-01'.
    For each individual, include: actual event rate, logistic risk score, xgboost risk score, risk classification,
    all applicable logistic variable coefficients, local SHAP value from xgboost.
    """
    import pandas as pd
    import numpy as np
    import json
    # Combine splits
    dfs = []
    for split_name, df in splits.items():
        if df is not None:
            df = df.copy()
            df['split'] = split_name
            dfs.append(df)
    all_df = pd.concat(dfs, axis=0, ignore_index=True)
    # Filter by effective_dates >= '2023-01-01' (handle type conversion)
    date_cutoff = pd.to_datetime('2023-01-01')
    if 'effective_month_start' in all_df.columns:
        all_df['effective_month_start'] = pd.to_datetime(all_df['effective_month_start'])
        all_df = all_df[all_df['effective_month_start'] >= date_cutoff]
    elif 'EFFECTIVE_MONTH_START' in all_df.columns:
        all_df['EFFECTIVE_MONTH_START'] = pd.to_datetime(all_df['EFFECTIVE_MONTH_START'])
        all_df = all_df[all_df['EFFECTIVE_MONTH_START'] >= date_cutoff]
    # Prepare output rows
    rows = []
    for idx, row in all_df.iterrows():
        fh_id = row.get('fh_id', row.get('FH_ID', None))
        effective_month_start = row.get('effective_month_start', row.get('EFFECTIVE_MONTH_START', None))
        actual_event = row.get('actual_any_event_next_90d', row.get('ANY_EVENT_NEXT_90D', None))
        split = row.get('split', None)
        # Get predictions
        logistic_prob = logistic_preds_dict.get(split, {}).get(fh_id, None)
        xgb_prob = xgb_preds_dict.get(split, {}).get(fh_id, None)
        # Risk classification logic (example: thresholds)
        risk_class = 'low'
        if logistic_prob is not None and logistic_prob >= 0.8:
            risk_class = 'high'
        elif logistic_prob is not None and logistic_prob >= 0.5:
            risk_class = 'medium'
        # Logistic coefficients for this individual
        coeffs = logistic_coeffs.get(fh_id, {})
        # SHAP values from xgboost
        shap_xgb = shap_xgb_dict.get(split, {}).get(fh_id, None)
        # Logistic risk score build-up
        logistic_score_build = []
        total_score = 0.0
        if coeffs and isinstance(coeffs, dict):
            for feat, coef in coeffs.items():
                if feat == 'intercept':
                    logistic_score_build.append({'feature': feat, 'value': 1, 'coefficient': coef, 'contribution': coef})
                    total_score += coef
                elif feat in row:
                    try:
                        val = float(row[feat])
                        contrib = val * coef
                        logistic_score_build.append({'feature': feat, 'value': val, 'coefficient': coef, 'contribution': contrib})
                        total_score += contrib
                    except Exception:
                        logistic_score_build.append({'feature': feat, 'value': None, 'coefficient': coef, 'contribution': None})
        # Optionally, add total score
        # XGBoost risk score build-up
        xgb_score_build = None
        if shap_xgb and isinstance(shap_xgb, dict):
            base = shap_xgb.get('base_value', 0.0)
            contribs = shap_xgb.get('contributions', [])
            xgb_score_build = {
                'base_value': base,
                'contributions': contribs,
                'total_score': base + sum(contribs)
            }
        rows.append({
            'fh_id': fh_id,
            'effective_month_start': effective_month_start,
            'actual_any_event_next_90d': actual_event,
            'split': split,
            'logistic.prob': logistic_prob,
            'xgboost.prob': xgb_prob,
            'risk_class': risk_class,
            'logistic_coefficients': json.dumps(coeffs),
            'logistic_score_build': json.dumps(logistic_score_build),
            'logistic_total_score': total_score,
            'xgboost_shap': json.dumps(shap_xgb),
            'xgboost_score_build': json.dumps(xgb_score_build)
        })
    out_df = pd.DataFrame(rows)
    output_path = cfg.get('FULL_INDIVIDUAL_OUTPUT_PATH', 'output/individual_full_consolidated.csv')
    out_df.to_csv(output_path, index=False)
    print(f"Full consolidated individual file written to {output_path}")
