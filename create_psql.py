from functools import partial

import pandas as pd

import psycopg2
import psycopg2.extras as extras

#### Some useful PSQL commands
# pg_ctl -D /usr/local/var/postgres start  --> start server
# psql mydatabasename

#### Monkeypatch psycopg2 functions

def safe_close(func):
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            cursor.close()
            raise Exception(e)
    return wrapper

@safe_close
def execute(conn, cursor, cmd, fetch=False):
    cursor.execute(cmd)
    conn.commit()
    if fetch:
        return cursor.fetchall()

@safe_close
def execute_batch(conn, cursor, cmd, tuples, page_size=100):
    extras.execute_batch(cursor, cmd, tuples, page_size)
    conn.commit()

def df_to_psql(conn, cursor, df, table_id):
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    schema = ','.join(len(df.columns) * ['%s'])

    cmd = f'CREATE TABLE IF NOT EXISTS {table_id}('
    for col in df.columns[:-1]:
        cmd += f'{col} VARCHAR( 255 ), '
    cmd += f'{df.columns[-1]} VARCHAR ( 255 )'
    cmd += ');'
    execute(conn, cursor, cmd)
    cmd = f'INSERT INTO {table_id}({cols}) VALUES({schema})'
    execute_batch(conn, cursor, cmd, tuples)

def psql_to_df(conn, cursor, query, column_names):
    """Tranform a SELECT query into a pandas dataframe"""
    data = execute(conn, cursor, query)
    df = pd.DataFrame(data, columns=column_names)
    return df

connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
               "password='neuroboothrocks'")
table_id = 'consent'
csv_fname = ('/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/'
             'register/consent.csv')

conn = psycopg2.connect(connect_str)
cursor = conn.cursor()

df = pd.read_csv(csv_fname)
df = df.where(~df.isna(), None)
df_to_psql(conn, cursor, df, table_id)

query = f'SELECT * FROM "{table_id}"'
column_names = df.columns  # XXX: hack
df_read = psql_to_df(conn, cursor, query, column_names)

cursor.close()
conn.close()
