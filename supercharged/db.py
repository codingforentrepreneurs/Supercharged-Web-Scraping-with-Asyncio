from sqlalchemy import create_engine

from .conf import DB_CONNECTION_STR

conn = create_engine(DB_CONNECTION_STR)

def verify_table_exists(table_name):
    return conn.dialect.has_table(conn, table_name)