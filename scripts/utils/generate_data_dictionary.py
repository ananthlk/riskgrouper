import os
import re
import pandas as pd
import sys
from pathlib import Path

# Add src directory to the Python path
project_root = Path(__file__).resolve().parents[2]
src_path = project_root / 'src'
sys.path.append(str(src_path))

from snowflake_connector import SnowflakeConnector

def find_sql_files(directory):
    """Finds all SQL files in a given directory."""
    sql_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.sql'):
                sql_files.append(os.path.join(root, file))
    return sql_files

def parse_sql_for_tables(file_content):
    """Parses SQL content to find table names, resolving variables."""
    # Find all SET variable assignments
    variables = dict(re.findall(r"SET\s+(\w+)\s*=\s*'([^']+)';", file_content, re.IGNORECASE))
    
    # Find all tables referenced directly or via IDENTIFIER()
    raw_tables = re.findall(r'(?:FROM|JOIN|INTO)\s+(?:IDENTIFIER\s*\(\s*\$(\w+)\s*\)|([\w\._]+))', file_content, re.IGNORECASE)
    
    tables = set()
    for var, direct in raw_tables:
        if var: # If it's a variable from IDENTIFIER($VAR)
            table_name = variables.get(var.upper())
            if table_name:
                tables.add(table_name.upper())
        elif direct: # If it's a direct table name
            # Exclude CTEs that might be captured
            if '.' in direct: # A simple check for FQNs vs CTEs
                 tables.add(direct.upper())
    return tables

def get_table_schema(connector, table_name):
    """Fetches the schema for a given table from Snowflake."""
    print(f"Fetching schema for table: {table_name}")
    try:
        query = f"DESCRIBE TABLE IDENTIFIER('{table_name}');"
        schema_df = connector.query_to_dataframe(query)
        return schema_df
    except Exception as e:
        print(f"Could not fetch schema for {table_name}: {e}")
        return None

def main():
    """Main function to generate the data dictionary."""
    sql_dir = 'scripts/sql'
    output_dir = 'docs'
    output_file = os.path.join(output_dir, 'data_dictionary.md')

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print("Starting data dictionary generation...")
    all_sql_files = find_sql_files(sql_dir)
    
    if not all_sql_files:
        print(f"No SQL files found in '{sql_dir}'. Exiting.")
        return

    all_tables = set()
    for sql_file in all_sql_files:
        with open(sql_file, 'r') as f:
            content = f.read()
            tables_in_file = parse_sql_for_tables(content)
            all_tables.update(tables_in_file)

    print(f"Found {len(all_tables)} unique tables to document.")

    with SnowflakeConnector() as sf, open(output_file, 'w') as f:
        f.write("# Data Dictionary\n\n")
        f.write("This document contains the schema for all tables used in the SQL scripts.\n\n")
        
        for table in sorted(list(all_tables)):
            schema = get_table_schema(sf, table)
            if schema is not None and not schema.empty:
                f.write(f"## ` {table} `\n\n")
                # Select and rename columns for clarity
                schema_to_write = schema[['name', 'type', 'comment']].rename(columns={
                    'name': 'Column Name',
                    'type': 'Data Type',
                    'comment': 'Description'
                })
                f.write(schema_to_write.to_markdown(index=False))
                f.write("\n\n")
            else:
                print(f"Skipping empty or failed schema for {table}")

    print(f"Data dictionary successfully generated at: {output_file}")

if __name__ == "__main__":
    main()
