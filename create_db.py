import os
import sqlite3
import pandas as pd
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

def create_tables_redshift(tables):
    # Create engine for Redshift
    redshift_engine = create_engine(f'postgresql://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}')
    
    # Connect to SQLite3
    sqlite_conn = sqlite3.connect(SQLITE3_DB_PATH)
    
    # Create tables in Redshift
    for table_name in tables:
        # Read table structure into DataFrame
        table_structure = pd.read_sql_query(f"PRAGMA table_info('{table_name}');", sqlite_conn)
        
        # Generate SQL CREATE TABLE statement for Redshift
        redshift_create_table_sql = f"CREATE TABLE IF NOT EXISTS {REDSHIFT_SCHEMA}.{table_name} ("
        for _, column in table_structure.iterrows():
            column_name = column['name']
            column_type = column['type']
            redshift_create_table_sql += f"{column_name} VARCHAR, "
        redshift_create_table_sql = redshift_create_table_sql[:-2] + ");"
        
        # Create table in Redshift
        with redshift_engine.connect() as conn:
            conn.execute(redshift_create_table_sql)
    
    # Close SQLite3 connection
    sqlite_conn.close()
    print("Tables created in Redshift.")

def create_tables_snowflake(tables):
    # Create engine for Snowflake
    snowflake_engine = create_engine(f'snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}')
    
    # Connect to SQLite3
    sqlite_conn = sqlite3.connect(SQLITE3_DB_PATH)
    
    # Create tables in Snowflake
    for table_name in tables:
        # Read table structure into DataFrame
        table_structure = pd.read_sql_query(f"PRAGMA table_info('{table_name}');", sqlite_conn)
        
        # Generate SQL CREATE TABLE statement for Snowflake
        snowflake_create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for _, column in table_structure.iterrows():
            column_name = column['name']
            column_type = column['type']
            snowflake_create_table_sql += f"{column_name} VARCHAR, "
        snowflake_create_table_sql = snowflake_create_table_sql[:-2] + ");"
        
        # Create table in Snowflake
        with snowflake_engine.connect() as conn:
            conn.execute(snowflake_create_table_sql)
    
    # Close SQLite3 connection
    sqlite_conn.close()
    print("Tables created in Snowflake.")

def copy_data_to_redshift(tables):
    # Create engine for Redshift
    redshift_engine = create_engine(f'postgresql://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DBNAME}')
    
    # Connect to SQLite3
    sqlite_conn = sqlite3.connect(SQLITE3_DB_PATH)
    
    # Copy tables and data to Redshift
    for table_name in tables:
        # Read data from SQLite3
        cursor = sqlite_conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        
        # Insert data into Redshift
        with redshift_engine.connect() as conn:
            for row in rows:
                values = ', '.join([f"'{value}'" for value in row])
                conn.execute(f"INSERT INTO {REDSHIFT_SCHEMA}.{table_name} VALUES ({values});")
    
    # Close SQLite3 connection
    sqlite_conn.close()
    print("Data copied to Redshift.")

def copy_data_to_snowflake(tables):
    # Create engine for Snowflake
    snowflake_engine = create_engine(f'snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}')
    
    # Connect to SQLite3
    sqlite_conn = sqlite3.connect(SQLITE3_DB_PATH)
    
    # Copy tables and data to Snowflake
    for table_name in tables:
        # Read data from SQLite3
        cursor = sqlite_conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()
        
        # Insert data into Snowflake
        with snowflake_engine.connect() as conn:
            for row in rows:
                values = ', '.join([f"'{value}'" for value in row])
                conn.execute(f"INSERT INTO {table_name} VALUES ({values});")
    
    # Close SQLite3 connection
    sqlite_conn.close()
    print("Data copied to Snowflake.")

if __name__ == "__main__":
    # Connect to SQLite3
    sqlite_conn = sqlite3.connect(SQLITE3_DB_PATH)
    
    # Get list of tables from SQLite3 database
    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", sqlite_conn)
    table_names = tables['name'].tolist()
    
    # Create tables in Redshift
    create_tables_redshift(table_names)
    
    # Create tables in Snowflake
    create_tables_snowflake(table_names)
    
    # Copy data to Redshift
    copy_data_to_redshift(table_names)
    
    # Copy data to Snowflake
    copy_data_to_snowflake(table_names)
    
    # Close SQLite3 connection
    sqlite_conn.close()

