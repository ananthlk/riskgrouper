from dotenv import load_dotenv
import os
from src.snowflake_connector import SnowflakeConnector

# Load environment variables from the .env file
load_dotenv()

def run_sql_script(query):
    """
    Executes a given SQL query on Snowflake and returns the results.

    Args:
        query (str): The SQL query to execute.

    Returns:
        list: Query results as a list of tuples.
    """
    with SnowflakeConnector() as sf:
        if sf.connection:
            return sf.execute_query(query)

if __name__ == "__main__":
    # Example usage
    query = "SELECT * FROM TRANSFORMED_DATA._TEMP.AL_REG_CONSOLIDATED_DATASET_NOTES_ONLY LIMIT 10;"
    results = run_sql_script(query)
    if results:
        for row in results:
            print(row)