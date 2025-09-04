"""
SQL Pipeline Runner

This script orchestrates the execution of a sequence of SQL files against a Snowflake
database. It is designed to run a data transformation pipeline where each SQL script
represents a step in the pipeline.

Key Features:
- **Sequential Execution**: Executes SQL scripts in a predefined, specific order to
  ensure data dependencies are met. The order is defined in the `SQL_SCRIPT_ORDER` list.
- **User-selectable Starting Point**: Prompts the user to select which script to start
  the execution from. This is highly useful for development and debugging, allowing the
  user to resume the pipeline from a specific step without re-running everything.
- **Dynamic Path Configuration**: Correctly resolves project paths to ensure that the
  `SnowflakeConnector` module can be imported regardless of where the script is run from.
- **Robust Error Handling**:
    - If a SQL script fails to execute, the pipeline halts immediately, and a detailed
      error message is printed. This prevents further data corruption or partial loads.
    - Gracefully handles user cancellation (Ctrl+C).
- **Context-Managed Connections**: Uses the `SnowflakeConnector` as a context manager
  (`with` statement) to ensure that the database connection is always closed properly,
  even if errors occur.

The script assumes that the SQL files are located in the `scripts/sql/` directory relative
to the project root.

Usage:
    Run the script from the command line from the project root directory:
    $ python scripts/utils/run_sql_pipeline.py
"""
import os
import sys
from pathlib import Path

# Add the 'src' directory to the Python path to allow for the import of the SnowflakeConnector.
# This makes the script runnable from any location.
project_root = Path(__file__).resolve().parents[2]
src_path = project_root / 'src'
sys.path.append(str(src_path))

from snowflake_connector import SnowflakeConnector

# Define the correct execution order of the SQL scripts.
# This list is critical as it defines the data dependency flow of the pipeline.
SQL_SCRIPT_ORDER = [
    "Prompts.sql",
    "baselines.sql",
    "Notes_agentic.sql",
    "events.sql",
    "daily_aggegation.sql",
    "data_prep.sql",
    "data_split.sql"
]

def get_user_selection(sql_scripts):
    """
    Prompts the user to select a starting script from a numbered list.

    This function displays the ordered list of SQL scripts and asks the user to
    input the number corresponding to the script where execution should begin.

    Args:
        sql_scripts (list[str]): The ordered list of SQL script filenames.

    Returns:
        list[str]: A sublist of `sql_scripts` starting from the user's selection.
                   Returns an empty list if the user cancels.
    """
    print("Please select the starting script for the pipeline execution:")
    for i, script in enumerate(sql_scripts):
        print(f"  {i + 1}: {script}")
    
    while True:
        try:
            start_num_str = input(f"Enter the number of the script to start from (1-{len(sql_scripts)}): ")
            start_num = int(start_num_str)
            if 1 <= start_num <= len(sql_scripts):
                # Return the slice of the list from the selected starting point.
                return sql_scripts[start_num - 1:]
            else:
                print(f"Invalid input. Please enter a number between 1 and {len(sql_scripts)}.")
        except ValueError:
            print("Invalid input. Please enter a number.")
        except (KeyboardInterrupt, EOFError):
            print("\nExecution cancelled by user.")
            return []

def execute_sql_file(connector, file_path):
    """
    Reads the content of a single SQL file and executes it.

    Args:
        connector (SnowflakeConnector): An active SnowflakeConnector instance.
        file_path (Path): The path to the SQL file to be executed.

    Returns:
        bool: True if execution was successful, False otherwise.
    """
    script_name = os.path.basename(file_path)
    print(f"\nExecuting: {script_name}...")
    try:
        with open(file_path, 'r') as f:
            sql_content = f.read()
            # The connector's `execute_string` method can handle multi-statement SQL scripts.
            # This is generally safe for Snowflake, which can process multiple DDL/DML statements in one call.
            connector.execute_string(sql_content)
        print(f"Successfully executed: {script_name}")
        return True
    except Exception as e:
        print(f"--- ERROR executing {script_name} ---")
        print(e)
        print("----------------------------------------------------")
        return False

def main():
    """
    Main function to drive the SQL pipeline execution.

    It gets the user's starting script selection, establishes a connection to Snowflake,
    and then iterates through the selected SQL scripts, executing them one by one.
    """
    # Define the directory where SQL scripts are stored.
    sql_dir = project_root / "scripts" / "sql"
    
    # Get the list of scripts to run based on user input.
    scripts_to_run = get_user_selection(SQL_SCRIPT_ORDER)
    
    if not scripts_to_run:
        return

    print("\nStarting SQL pipeline execution...")
    
    try:
        # Use the SnowflakeConnector as a context manager to ensure the connection is closed.
        with SnowflakeConnector() as sf:
            if not sf.connection:
                print("Failed to connect to Snowflake. Aborting pipeline.")
                return

            for script_name in scripts_to_run:
                script_path = sql_dir / script_name
                if not script_path.exists():
                    print(f"Warning: Script '{script_name}' not found at '{script_path}'. Skipping.")
                    continue
                
                success = execute_sql_file(sf, script_path)
                if not success:
                    print("\nPipeline execution halted due to an error.")
                    break
        
        print("\nSQL pipeline execution finished.")

    except Exception as e:
        print(f"\nA critical error occurred during the pipeline setup or connection: {e}")

if __name__ == "__main__":
    main()
