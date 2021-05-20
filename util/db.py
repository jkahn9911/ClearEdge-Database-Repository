import pyodbc
import pandas as pd
import numpy as np
import collections
from .config import db_dev, db_id, db_prod, db_server

pyodbc.lowercase = True  # all column names to lowercase

def open_conn(is_prod = False):
    if is_prod:
        database = db_prod
    else:
        database = db_dev
    try:
        con_str = "DRIVER={ODBC Driver 17 for SQL Server};Server="+db_server+";database="+database+";UID="+db_id+";Authentication=ActiveDirectoryInteractive;"
        cnxn = pyodbc.connect(con_str)
    except pyodbc.DatabaseError as err:
        return err
    DB_CONN = collections.namedtuple('DB_CONN', 'cnxn cursor')
    return DB_CONN(cnxn = cnxn, cursor = cnxn.cursor())


def close_conn(conn_tup):
    conn_tup.cnxn.close()


def get_table_pd(q, conn_tup):
    try:
        conn_tup.cursor.execute(q)
    except pyodbc.DatabaseError as err:
        return err
    rows = conn_tup.cursor.fetchall()
    if not rows:
        raise Exception('No records found.')
    col_names = [name[0] for name in conn_tup.cursor.description]
    full_table = pd.DataFrame(np.asarray(rows), columns = col_names)
    full_table = full_table.convert_dtypes()

    return full_table


def get_columns(table_name, conn_tup, schema='dbo'):
    return [row.column_name for row in conn_tup.cursor.columns(table=table_name, schema=schema)]


def insert_into(table, values, conn_tup, columns=""):
    if columns:
        col_len = len(columns)
        columns = "("+", ".join(columns)+") "
    else:
        get_columns(table, conn_tup=conn_tup)
        col_len = len(columns)
        columns = "("+", ".join(columns)+") "
    val_str = "("+", ".join("?"*col_len) +")"
    q = "INSERT INTO "+table+" "+columns+"VALUES "+val_str
    try:
        conn_tup.cursor.executemany(q, values)
    except pyodbc.DatabaseError as err:
        conn_tup.cnxn.rollback()
        print(err)
    else:
        conn_tup.cnxn.commit()