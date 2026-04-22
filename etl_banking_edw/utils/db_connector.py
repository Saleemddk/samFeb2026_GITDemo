"""
Database connector utilities — MySQL (source) and Oracle (target).
"""

import mysql.connector
import oracledb
from config.database_config import SOURCE_DB, TARGET_DB


def get_mysql_connection():
    """Return a MySQL connection to the source OLTP database."""
    return mysql.connector.connect(
        host=SOURCE_DB["host"],
        port=SOURCE_DB["port"],
        user=SOURCE_DB["user"],
        password=SOURCE_DB["password"],
        database=SOURCE_DB["database"],
    )


def get_oracle_connection():
    """Return an Oracle connection to the target warehouse."""
    dsn = f"{TARGET_DB['host']}:{TARGET_DB['port']}/{TARGET_DB['service']}"
    return oracledb.connect(
        user=TARGET_DB["user"],
        password=TARGET_DB["password"],
        dsn=dsn,
    )


def run_mysql_query(query: str, params=None) -> list:
    """Execute a SELECT query against MySQL and return all rows."""
    conn = get_mysql_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(query, params or ())
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


def run_oracle_query(query: str, params=None) -> list:
    """Execute a SELECT query against Oracle and return all rows."""
    conn = get_oracle_connection()
    cursor = conn.cursor()
    cursor.execute(query, params or ())
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return rows
