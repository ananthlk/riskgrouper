import pandas as pd
import sys
import os
sys.path.append(os.path.abspath('src'))
from snowflake_connector import SnowflakeConnector

def run_analysis():
    """
    Runs the top conditions analysis and prints the results.
    """
    try:
        # Read the SQL query
        with open('top_conditions_analysis.sql', 'r') as f:
            query = f.read()

        # Execute the query using the connector
        print("Connecting to Snowflake and running analysis...")
        with SnowflakeConnector() as connector:
            if connector.connection:
                df = connector.query_to_dataframe(query)
                print("Analysis complete. Top 5 conditions:")
                print(df)
            else:
                print("Failed to connect to Snowflake.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run_analysis()
