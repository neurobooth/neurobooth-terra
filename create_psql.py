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
def execute(cursor, cmd, fetch=False):
    cursor.execute(cmd)
    cursor.commit()
    if fetch:
        return cursor.fetchall()

@safe_close
def execute_batch(cursor, cmd, tuples, page_size=100):
    extras.execute_batch(cursor, cmd, tuples, page_size)
    cursor.commit()

def df_to_psql(cursor, df, table_id):
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    schema = ','.join(len(df.columns) * ['%s'])
    cmd = f'INSERT INTO {table_id}({cols}) VALUES({schema})'
    execute_batch(cursor, cmd, tuples)


connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
               "password='neuroboothrocks'")
table_id = 'consent'
csv_fname = ('/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/'
             'register/consent.csv')

conn = psycopg2.connect(connect_str)
cursor = conn.cursor()

df = pd.read_csv(csv_fname)
df = df.where(~df.isna(), None)
df_to_psql(cursor, df, table_id)

cursor.close()
conn.close()
