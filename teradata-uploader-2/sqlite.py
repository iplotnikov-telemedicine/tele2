import sqlite3
from datetime import datetime
import streamlit as st
import pandas as pd


@st.experimental_singleton
def create_connection(db_file):
    _conn = None
    try:
        _conn = sqlite3.Connection(db_file, check_same_thread=False)
    except sqlite3.Error as e:
        print(e)
    return _conn


def get_columns(_conn, table):
    cursor = _conn.cursor()
    sql = f'PRAGMA table_info({table});'
    cursor.execute(sql)
    columns = [col[1] for col in cursor.fetchall()]
    cursor.close()
    return columns


def add_record(_conn, filename, database, table, username):
    columns = get_columns(_conn, 'HISTORY')
    values = []  
    for col in columns:
        if col == 'Таблица':
            values.append(table)
        elif col == 'Схема':
            values.append(database)
        elif col == 'Имя пользователя':
            values.append(username)
        elif col == 'Имя файла':
            values.append(filename)
        elif col == 'Дата и время':
            values.append(datetime.now().replace(microsecond=0))
        else:
            values.append(None)
    sql = f'''
    INSERT INTO HISTORY ({', '.join('"{}"'.format(col) for col in columns)}) 
    VALUES ({ ','.join('?'*len(values))});
    '''
    _conn.execute(sql, values)
    _conn.commit()
    return


def get_history(_conn, username):
    sql = f'''
        SELECT "Дата и время", "Имя файла", "Схема", "Таблица"
        FROM HISTORY
        WHERE "Имя пользователя"=:username
        ORDER BY "Дата и время" DESC
        '''
    return pd.read_sql_query(sql, _conn, params={'username':username})