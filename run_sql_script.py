
import argparse
import os
from src.snowflake_connector import SnowflakeConnector
import pandas as pd

def run_sql_from_file(filepath, output_path=None):
    """
    Reads a SQL file, splits it into individual statements, and executes them
    using the SnowflakeConnector.

    Args:
        filepath (str): The full path to the .sql file.
        output_path (str, optional): The path to save the final query result as a CSV.
    """
    # 1. Check if the file exists
    if not os.path.exists(filepath):
        print(f"Error: SQL script file not found at '{filepath}'")
        return

    # 2. Read the entire SQL script
    try:
        with open(filepath, 'r') as f:
            sql_script = f.read()
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        return

    # 3. Split the script into individual statements, handling semicolons
    #    and removing empty statements that result from splitting.
    statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]

    if not statements:
        print("No SQL statements found in the file.")
        return

    # 4. Use the SnowflakeConnector to execute the statements
    with SnowflakeConnector() as sf:
        if not sf.connection:
            print("Failed to connect to Snowflake. Aborting script execution.")
            return

        print(f"Successfully connected to Snowflake. Executing {len(statements)} statements from '{os.path.basename(filepath)}'...")

        final_df = None  # To store the result of the last SELECT statement

        for i, statement in enumerate(statements):
            print(f"\n--- Executing Statement {i+1}/{len(statements)} ---")
            print(statement)
            
            try:
                # Clean the statement by removing any leading comments to properly
                # identify its type (e.g., SELECT vs. SET).
                cleaned_statement = '\n'.join(
                    line for line in statement.strip().split('\n') 
                    if not line.strip().startswith('--')
                ).strip()

                # Check if the cleaned statement is a SELECT query to fetch results
                if cleaned_statement.upper().startswith('SELECT'):
                    df = sf.query_to_dataframe(statement)
                    if df is not None:
                        print("Query Result:")
                        # Use to_string() to ensure the full DataFrame is printed
                        print(df.to_string())
                        final_df = df  # Store the dataframe
                    else:
                        print("Query executed but returned no data.")
                # For other statements (SET, CREATE, etc.), just execute
                else:
                    sf.execute_query(statement)
                    print("Statement executed successfully.")
            except Exception as e:
                print(f"An error occurred while executing statement: {e}")
                print("Aborting due to error.")
                break
        
        # 5. Save the final DataFrame to CSV if output_path is provided
        if final_df is not None and output_path:
            try:
                # Create directory if it doesn't exist
                output_dir = os.path.dirname(output_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                
                final_df.to_csv(output_path, index=False)
                print(f"\n--- Successfully saved query result to '{output_path}' ---")
            except Exception as e:
                print(f"\n--- Error saving result to CSV: {e} ---")

        print("\n--- Script execution finished ---")

def main():
    """
    Main function to parse command-line arguments and trigger the SQL script execution.
    """
    parser = argparse.ArgumentParser(description="Run a SQL script against Snowflake.")
    parser.add_argument("--script_path", required=True, help="The path to the .sql script to execute.")
    parser.add_argument("--output_path", required=False, help="The path to save the final query result as a CSV.")
    
    args = parser.parse_args()
    
    run_sql_from_file(args.script_path, args.output_path)

if __name__ == "__main__":
    main()
