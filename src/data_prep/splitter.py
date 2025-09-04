import pandas as pd
# splitter.py
# Member-level train/val/test split logic for Risk Grouper Refactor
from typing import Dict
from sklearn.model_selection import train_test_split

def split_data(df: pd.DataFrame, config: dict) -> Dict[str, pd.DataFrame]:
    """
    Wrapper for member_level_split using config dict for target and seed.
    """
    target = config.get('TARGET', 'ip_event_next_90d')
    seed = config.get('SEED', 42)
    # Optionally allow config for test/val sizes
    test_size = config.get('TEST_SIZE', 0.2)
    val_size = config.get('VAL_SIZE', 0.25)
    return member_level_split(df, target, test_size=test_size, val_size=val_size, seed=seed)
# splitter.py
# Member-level train/val/test split logic for Risk Grouper Refactor

def member_level_split(
    df: pd.DataFrame,
    target: str,
    test_size: float = 0.2,
    val_size: float = 0.25,
    seed: int = 42
) -> Dict[str, pd.DataFrame]:
    """
    Splits df into train/val/test at member (fh_id) level, returns dict of splits.
    """
    unique_members = df['FH_ID'].unique()
    train_members, test_members = train_test_split(unique_members, test_size=test_size, random_state=seed)
    train_members, val_members = train_test_split(train_members, test_size=val_size, random_state=seed)
    train_df = df[df['FH_ID'].isin(train_members)].copy().dropna(subset=[target])
    val_df = df[df['FH_ID'].isin(val_members)].copy().dropna(subset=[target])
    test_df = df[df['FH_ID'].isin(test_members)].copy().dropna(subset=[target])
    return {'train': train_df, 'val': val_df, 'test': test_df}
