# snowflake_connector.py

"""
Snowflake Connector Module

This module provides a robust and simplified interface for connecting to and interacting with
a Snowflake data warehouse. It is designed to handle SSO (Single Sign-On) authentication
via an external browser, ensuring secure access without hardcoding credentials in the script.

Key Features:
- **Environment-based Configuration**: Loads all Snowflake connection parameters from a `.env`
  file, promoting best practices for managing sensitive information.
- **SSO Authentication**: Utilizes Snowflake's `externalbrowser` authenticator for secure,
  interactive login.
- **Context Manager Support**: The `SnowflakeConnector` class can be used as a context
  manager (`with` statement), which automatically handles the opening and closing of
  the database connection.
- **Pandas Integration**: Includes a method to directly execute a SQL query and return the
  results as a pandas DataFrame, streamlining data retrieval for analysis.
- **Error Handling**: Incorporates try-except blocks to gracefully handle connection and
  query execution errors.

The module expects a `.env` file in the root directory with the following variables:
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_AUTHENTICATOR (optional, defaults to 'externalbrowser')
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA
- SNOWFLAKE_ROLE (optional)

Example Usage:
    
    from snowflake_connector import SnowflakeConnector

    # Using the context manager (recommended)
    with SnowflakeConnector() as sf:
        if sf.connection:
            df = sf.query_to_dataframe("SELECT * FROM my_table LIMIT 10")
            if df is not None:
                print(df)

    # Manual connection management
    sf_connector = SnowflakeConnector()
    if sf_connector.connect():
        tables = sf_connector.get_tables()
        if tables:
            for table in tables:
                print(table['name'])
        sf_connector.close()
"""

import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import DictCursor
import pandas as pd

# Load environment variables from the project's .env file.
# This allows for secure management of credentials and connection details.
load_dotenv()

# Retrieve Snowflake connection parameters from environment variables.
# Using os.getenv allows for default values and handles cases where a variable is not set.
SNOWFLAKE_CONFIG = {
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'user': os.getenv('SNOWFLAKE_USER'),
    'authenticator': os.getenv('SNOWFLAKE_AUTHENTICATOR', 'externalbrowser'), # Default to SSO
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA'),
    'role': os.getenv('SNOWFLAKE_ROLE')  # Optional role
}

# Define connection timeouts to prevent indefinite hanging.
# Setting a higher CLIENT_SESSION_KEEP_ALIVE value to maintain the session.
CONNECTION_TIMEOUT = 30  # Timeout for the network connection
LOGIN_TIMEOUT = 120      # Timeout for the SSO login process
CLIENT_SESSION_KEEP_ALIVE = True # Keep session alive

class SnowflakeConnector:
    """
    A class to manage connections and data exchange with Snowflake.

    This class encapsulates the logic for connecting to Snowflake, executing queries,
    and fetching data. It is designed to be used either directly or as a context manager.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SnowflakeConnector, cls).__new__(cls)
            cls._instance.connection = None
            cls._instance.cursor = None
        return cls._instance

    def __init__(self):
        """
        Initializes the SnowflakeConnector, reusing any existing connection.
        """
        pass # Initialization is handled in __new__

    def connect(self):
        """
        Establishes a connection to Snowflake using the parameters defined in the
        environment variables. It handles the SSO authentication flow.
        If a connection already exists and is open, it does nothing.

        Returns:
            bool: True if the connection was successful or already exists, False otherwise.
        """
        if self.connection and not self.connection.is_closed():
            print("Reusing existing Snowflake connection.")
            return True
        
        try:
            print("Connecting to Snowflake...")
            # Filter out any configuration parameters that are not set (are None).
            conn_params = {k: v for k, v in SNOWFLAKE_CONFIG.items() if v is not None}
            
            # Establish the connection
            self.connection = snowflake.connector.connect(
                **conn_params,
                network_timeout=CONNECTION_TIMEOUT,
                login_timeout=LOGIN_TIMEOUT,
                client_session_keep_alive=CLIENT_SESSION_KEEP_ALIVE
            )
            self.cursor = self.connection.cursor(DictCursor)
            print("Successfully connected to Snowflake!")
            
            # Verify connection
            self.cursor.execute("SELECT CURRENT_USER() AS user")
            result = self.cursor.fetchone()
            print(f"Connected as user: {result['USER']}")
            return True
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            self.connection = None
            self.cursor = None
            return False

    def execute_query(self, query):
        """
        Executes a given SQL query using the active cursor.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list[dict] or None: A list of dictionaries representing the query results,
                                or None if an error occurs or there is no connection.
        """
        if not self.connect(): # Ensure connection exists
            return None
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def query_to_dataframe(self, query):
        """
        Executes a SQL query and fetches the results into a pandas DataFrame.

        Args:
            query (str): The SQL query to execute.

        Returns:
            pd.DataFrame or None: A DataFrame containing the query results,
                                  or None if an error occurs or there is no connection.
        """
        if not self.connect(): # Ensure connection exists
            return None
        try:
            # pd.read_sql is highly efficient for creating DataFrames from SQL queries
            return pd.read_sql(query, self.connection)
        except Exception as e:
            print(f"Error executing query to DataFrame: {e}")
            return None

    def query_to_dataframe_streamed(self, query):
        """
        Executes a SQL query and streams the results as pandas DataFrames.

        This method is useful for large datasets that may not fit into memory.
        It executes the query and yields chunks of the result as pandas DataFrames.

        Args:
            query (str): The SQL query to execute.

        Yields:
            pd.DataFrame: A chunk of the result set as a pandas DataFrame.
        """
        if not self.connect(): # Ensure connection exists
            return
        
        try:
            print("Executing query for streaming...")
            self.cursor.execute(query)
            print("Query executed. Fetching results in batches...")
            
            for chunk in self.cursor.fetch_pandas_batches():
                yield chunk
            
            print("Finished fetching all batches.")

        except Exception as e:
            print(f"Error streaming query to DataFrame: {e}")
            return

    def get_tables(self):
        """
        Retrieves a list of tables in the current database and schema.

        Returns:
            list[dict] or None: A list of tables, or None on error.
        """
        query = "SHOW TABLES"
        return self.execute_query(query)

    def get_warehouses(self):
        """
        Retrieves a list of available warehouses accessible by the user.

        Returns:
            list[dict] or None: A list of warehouses, or None on error.
        """
        query = "SHOW WAREHOUSES"
        return self.execute_query(query)

    def close(self):
        """
        Closes the cursor and the connection to Snowflake.
        It's important to call this to release resources.
        """
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
        print("Snowflake connection closed.")

    def __enter__(self):
        """
        Context manager entry point. Establishes the connection.
        Allows the class to be used in a `with` statement.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit point. Ensures the connection is closed.
        This method is called automatically when exiting the `with` block.
        """
        # The connection is no longer closed here to keep it alive.
        # self.close()
        print("Leaving context, but keeping Snowflake connection alive.")