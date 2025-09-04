import os
import pandas as pd
from datetime import datetime, timedelta

# --- Cache Settings ---
CACHE_DIR = "cache"
CACHE_EXPIRATION_HOURS = 24  # How long before the cache is considered stale

def _get_cache_filepath(query_name: str) -> str:
    """Constructs the full path for a given cache file."""
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    return os.path.join(CACHE_DIR, f"{query_name}.parquet")

def is_cache_valid(query_name: str, force_refresh: bool = False) -> bool:
    """
    Checks if a valid, non-stale cache file exists for a given query.

    Args:
        query_name: A unique name for the query (e.g., 'events_data').
        force_refresh (bool): If True, invalidates the cache and forces a refresh.

    Returns:
        True if a valid cache file exists, False otherwise.
    """
    if force_refresh:
        print("User requested a force refresh.")
        return False
        
    cache_path = _get_cache_filepath(query_name)
    if not os.path.exists(cache_path):
        print("Cache does not exist.")
        return False

    file_mod_time = datetime.fromtimestamp(os.path.getmtime(cache_path))
    if datetime.now() - file_mod_time > timedelta(hours=CACHE_EXPIRATION_HOURS):
        print(f"Cache is stale (older than {CACHE_EXPIRATION_HOURS} hours).")
        return False

    # Ask user if they want to use the cache
    while True:
        try:
            choice = input(f"A valid cache for '{query_name}' is available. Do you want to use it? (y/n): ").lower().strip()
            if choice in ['y', 'yes']:
                print("Using cached data.")
                return True
            elif choice in ['n', 'no']:
                print("User opted to refresh data from source.")
                return False
            else:
                print("Invalid input. Please enter 'y' or 'n'.")
        except EOFError: # Handles cases where input stream is closed (e.g. in non-interactive scripts)
            print("No user input detected, defaulting to using cache.")
            return True

def load_from_cache(query_name: str) -> pd.DataFrame:
    """
    Loads data from a Parquet cache file.

    Args:
        query_name: The unique name for the query.

    Returns:
        A pandas DataFrame with the cached data.
    """
    cache_path = _get_cache_filepath(query_name)
    print(f"Loading data from cache: {cache_path}")
    return pd.read_parquet(cache_path)

def save_to_cache(df: pd.DataFrame, query_name: str):
    """
    Saves a DataFrame to a Parquet cache file.

    Args:
        df: The pandas DataFrame to save.
        query_name: The unique name for the query.
    """
    cache_path = _get_cache_filepath(query_name)
    print(f"Saving data to cache: {cache_path}")
    df.to_parquet(cache_path, index=False)
