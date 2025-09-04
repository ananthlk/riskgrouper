
import pandas as pd
from src.snowflake_connector import SnowflakeConnector

def get_table_definition():
    """
    Connects to Snowflake and retrieves the definition of a specific table.
    """
    query = "DESCRIBE TABLE TRANSFORMED_DATA.PROD_SNAPSHOT.MEMBER_RAF_YEARLY_SNAPSHOT"
    
    print(f"Executing query: {query}")
    
    with SnowflakeConnector() as sf:
        if sf.connection:
            try:
                # Using query_to_dataframe to get a nice format
                df = sf.query_to_dataframe(query)
                if df is not None:
                    print("Table Definition:")
                    print(df.to_string())
                else:
                    print("Could not retrieve table definition.")
            except Exception as e:
                print(f"An error occurred: {e}")

if __name__ == "__main__":
    get_table_definition()
