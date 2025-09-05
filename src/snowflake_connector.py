# snowflake_connector.py

"""
Snowflake connector utilities with SSO authentication
"""
import snowflake.connector
from snowflake.connector import DictCursor
import pandas as pd

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    'account': 'BWA25349.us-east-1',  # Region format based on login URL
    'user': 'ananth@firsthandcares.com',
    'authenticator': 'externalbrowser',
    'warehouse': 'DATASCIENCE',
    'database': 'TRANSFORMED_DATA',
    'schema': '_TEMP',
    'role': 'DATASCIENTIST'  # Optional: specify a role
}

# Connection timeout settings
CONNECTION_TIMEOUT = 30
LOGIN_TIMEOUT = 120

class SnowflakeConnector:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Establish connection to Snowflake using SSO authentication and run a basic query.
        """
        try:
            print("Initiating SSO login to Snowflake...")
            print("A browser window will open for authentication.")
            self.connection = snowflake.connector.connect(
                account=SNOWFLAKE_CONFIG['account'],
                user=SNOWFLAKE_CONFIG['user'],
                authenticator=SNOWFLAKE_CONFIG['authenticator'],
                warehouse=SNOWFLAKE_CONFIG['warehouse'],
                database=SNOWFLAKE_CONFIG['database'],
                schema=SNOWFLAKE_CONFIG['schema'],
                role=SNOWFLAKE_CONFIG.get('role'),
                network_timeout=CONNECTION_TIMEOUT,
                login_timeout=LOGIN_TIMEOUT
            )
            self.cursor = self.connection.cursor(DictCursor)
            print("Successfully connected to Snowflake!")
            # Run a basic query to verify connection
            try:
                self.cursor.execute("SELECT CURRENT_USER() AS user")
                result = self.cursor.fetchone()
                print(f"Connected as user: {result['USER']}")
            except Exception as qe:
                print(f"Query error: {qe}")
            return True
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            return False

    def execute_query(self, query):
        """
        Execute a SQL query and return results
        """
        if not self.cursor:
            print("No active connection. Please connect first.")
            return None
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def query_to_dataframe(self, query):
        """
        Execute a query and return results as a pandas DataFrame
        """
        if not self.connection:
            print("No active connection. Please connect first.")
            return None
        try:
            return pd.read_sql(query, self.connection)
        except Exception as e:
            print(f"Error executing query to DataFrame: {e}")
            return None

    def get_tables(self):
        """
        Get list of tables in the current database/schema
        """
        query = "SHOW TABLES"
        return self.execute_query(query)

    def get_warehouses(self):
        """
        Get list of available warehouses
        """
        query = "SHOW WAREHOUSES"
        return self.execute_query(query)

    def close(self):
        """
        Close the connection
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection closed.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()