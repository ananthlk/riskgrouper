# features.py
# Feature selection, exclusion, and type validation for Risk Grouper Refactor

import pandas as pd
import logging
from typing import List, Tuple

def select_features(df: pd.DataFrame, exclude: list, logger: logging.Logger) -> Tuple[List[str], List[str]]:
    """
    Returns list of numeric features not in exclude, and list of skipped features (non-numeric or missing).
    """
    features = []
    skipped = []
    # Get target from config if available
    import src.config
    target = getattr(src.config, 'DEFAULT_CONFIG', {}).get('TARGET', None)
    # Normalize exclude list for case and whitespace
    exclude_norm = set(e.strip().upper() for e in exclude)
    target_norm = target.strip().upper() if target else None
    for col in df.columns:
        col_norm = col.strip().upper()
        # Exclude target and excluded features (case-insensitive, strip whitespace)
        if col_norm == target_norm:
            continue
        if col_norm in exclude_norm:
            continue
        # Only include numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            features.append(col)
        else:
            skipped.append(col)
            logger.warning(f"Skipping non-numeric feature: {col}")
    return features, skipped
