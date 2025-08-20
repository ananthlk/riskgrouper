"""
Data Validation Runner

This script exec            # We split by the single semicolon used as a delimiter.
            sql_content = f.read()
            queries = [q.strip() for q in sql_content.split(';') if q.strip()]s the validation queries defined in `data_validation.sql`
and prints the results of each check to the console. It is designed to be run
as a standalone check to ensure data quality and integrity throughout the pipeline.

Key Features:
- **Automated Execution**: Runs all validation checks from the specified SQL file.
- **Clear Reporting**: Parses the SQL file, executes each validation query
  individually, and prints the results in a structured and readable format.
- **Pandas Integration**: Uses pandas DataFrames to neatly display the results
  of each validation query.
- **Error Handling**: Will report any errors encountered during the execution
  of a specific validation query.
"""
import os
import sys
import re
from pathlib import Path
import pandas as pd

# Add the 'src' directory to the Python path to allow for the import of the SnowflakeConnector.
project_root = Path(__file__).resolve().parents[2]
src_path = project_root / 'src'
sys.path.append(str(src_path))

from snowflake_connector import SnowflakeConnector

# Define the path to the validation SQL script.
VALIDATION_SCRIPT_PATH = project_root / "scripts" / "validation" / "data_validation.sql"

def run_validation_checks():
    """
    Reads, parses, and executes the validation SQL script, printing the results.
    """
    if not VALIDATION_SCRIPT_PATH.exists():
        print(f"Error: Validation script not found at {VALIDATION_SCRIPT_PATH}")
        return

    print(f"--- Running Data Validation Script: {VALIDATION_SCRIPT_PATH.name} ---")

    try:
        with open(VALIDATION_SCRIPT_PATH, 'r') as f:
            sql_content = f.read()

        # Remove block comments (/* ... */) using regex
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)
        
        # Split the script into individual queries using semicolon as a delimiter
        # and filter out any empty strings that result from the split.
        queries = [q.strip() for q in sql_content.split(';') if q.strip()]

        with SnowflakeConnector() as sf:
            if not sf.connection:
                print("Failed to connect to Snowflake. Aborting validation.")
                return

            for query in queries:
                # Remove line comments (-- ...) from each individual query
                query = re.sub(r'--.*', '', query).strip()
                if not query:
                    continue

                print("\n" + "="*80)
                # Extract the test name from the query for clear reporting
                try:
                    test_name_line = next(line for line in query.split('\n') if "' AS test_name" in line)
                    test_name = test_name_line.split("'")[1]
                    print(f"Executing Test: {test_name}")
                except StopIteration:
                    print("Executing Query...")
                
                print(f"Query:\n{query[:200]}...") # Print a snippet of the query

                try:
                    # Execute the query and display the result as a DataFrame
                    result_df = sf.query_to_dataframe(query)
                    
                    if result_df is None:
                         print("Query did not return results or failed.")
                    elif result_df.empty:
                        print("Result: OK (No issues found)")
                    else:
                        print("Result: ISSUES FOUND")
                        print(result_df.to_string())

                except Exception as e:
                    print(f"--- ERROR executing query ---")
                    print(e)
                    print("-----------------------------")

    except Exception as e:
        print(f"\nA critical error occurred: {e}")

    print("\n" + "="*80)
    print("--- Data Validation Script Finished ---")


if __name__ == "__main__":
    run_validation_checks()
