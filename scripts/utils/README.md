# Utility Scripts

This directory contains utility scripts that support the main data pipeline and modeling process.

## Scripts

- `run_analysis.py`: A command-line utility to execute a SQL script against Snowflake. It handles the connection, executes the script, and prints the results of any `SELECT` statements. This is the primary tool for running the main SQL pipeline and validation scripts.

- `run_sql_script.py`: A Python module that provides a function to execute a given SQL script. It is used by `run_analysis.py`.

- `get_table_def.py`: A script to fetch and display the DDL (Data Definition Language) for a specified table in Snowflake. This is useful for debugging and understanding the structure of the tables created by the pipeline.
