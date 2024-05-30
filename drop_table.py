import pandas as pd
import os
import sqlite3
from sqlalchemy import create_engine

# Load environment variables from file
from dotenv import load_dotenv
load_dotenv('.env')

# SQLite3
SQLITE3_DB_PATH = os.getenv("SQLITE3_DB_PATH")

# Redshift
REDSHIFT_DBNAME = os.getenv("REDSHIFT_DBNAME")
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")

# Snowflake
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

def drop_tables_redshift(tables):
    # Create engine for Redshift
    redshift_engine = create_engine(f'postgresql://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}')
    
    # Drop tables in Redshift schema if they exist in SQLite3
    with redshift_engine.connect() as conn:
        for table in tables:
            conn.execute(f"DROP TABLE IF EXISTS {REDSHIFT_SCHEMA}.{table};")

def drop_tables_snowflake(tables):
    # Create engine for Snowflake
    snowflake_engine = create_engine(f'snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}')
    
    # Drop tables in Snowflake schema if they exist in SQLite3
    with snowflake_engine.connect() as conn:
        for table in tables:
            conn.execute(f"DROP TABLE IF EXISTS {table};")

if __name__ == "__main__":
    # Connect to SQLite3
    sqlite_conn = sqlite3.connect(SQLITE3_DB_PATH)
    
    # Get list of tables from SQLite3 database
    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", sqlite_conn)
    table_names = tables['name'].tolist()
    
    # Drop tables in Redshift
    drop_tables_redshift(table_names)
    print("Tables dropped in Redshift.")
    
    # Drop tables in Snowflake
    drop_tables_snowflake(table_names)
    print("Tables dropped in Snowflake.")
    
    # Close SQLite3 connection
    sqlite_conn.close()

