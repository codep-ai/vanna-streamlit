# connect_db.py
import streamlit as st
import snowflake.connector
import psycopg2
import sqlite3
import duckdb

# Function to connect to Snowflake
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["SNOWFLAKE_DATABASE"],
        role=st.secrets["SNOWFLAKE_ROLE"],
        schema=st.secrets["SNOWFLAKE_SCHEMA"]
    )
    st.write("Connected to Snowflake")
    return conn

# Function to connect to Redshift
def connect_to_redshift():
    conn = psycopg2.connect(
        dbname=st.secrets["REDSHIFT_DBNAME"],
        user=st.secrets["REDSHIFT_USER"],
        password=st.secrets["REDSHIFT_PASSWORD"],
        host=st.secrets["REDSHIFT_HOST"],
        port=st.secrets["REDSHIFT_PORT"]
    )

    st.write("Connected to Redshift")
    return conn

# Function to connect to SQLite3
def connect_to_sqlite():
    conn = sqlite3.connect(st.secrets["SQLITE3_DB_PATH"])
    st.write("Connected to SQLite3")
    return conn

# Function to connect to DuckDB
def connect_to_duckdb():
    conn = duckdb.connect(database=st.secrets["DUCKDB_DB_PATH"])
    st.write("Connected to DuckDB")
    return conn

# Common function to connect to a database based on db_type
def connect_to_db(db_type):
    if db_type == "Snowflake":
        return connect_to_snowflake()
    elif db_type == "Redshift":
        return connect_to_redshift()
    elif db_type == "SQLite3":
        return connect_to_sqlite()
    elif db_type == "DuckDB":
        return connect_to_duckdb()
    else:
        st.write("Unsupported database type")
        return None

