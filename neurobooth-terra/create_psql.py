# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import pandas as pd

import psycopg2
import psycopg2.extras as extras

#### Some useful PSQL commands
# pg_ctl -D /usr/local/var/postgres start  --> start server
# psql mydatabasename

#### Monkeypatch psycopg2 functions ####

def safe_close(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
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
def _execute_batch(conn, cursor, cmd, tuples, page_size=100):
    extras.execute_batch(cursor, cmd, tuples, page_size)
    conn.commit()

#### Neurobooth related comands #####

def df_to_psql(conn, cursor, df, table_id):
    """Convert a dataframe to a Postgres SQL table

    Parameters
    ----------
    conn : instance of psycopg2.Postgres
        The connection object
    cursor : instance of psycopg2.cursor
        The cursor object
    df : instance of pd.Dataframe
        The dataframe to insert into the table
    table_id : str
        The table_id to create
    """
    df = df.where(~df.isna(), None)
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    vals = ','.join(len(df.columns) * ['%s'])

    create_cmd = f'CREATE TABLE IF NOT EXISTS {table_id}('
    for col in df.columns[:-1]:
        create_cmd += f'{col} VARCHAR( 255 ), '
    create_cmd += f'{df.columns[-1]} VARCHAR ( 255 )'
    create_cmd += ');'
    execute(conn, cursor, create_cmd)

    insert_cmd = f'INSERT INTO {table_id}({cols}) VALUES({vals})'
    _execute_batch(conn, cursor, insert_cmd, tuples)

def psql_to_df(conn, cursor, query, column_names):
    """Tranform a SELECT query into a pandas dataframe"""
    data = execute(conn, cursor, query, fetch=True)
    df = pd.DataFrame(data, columns=column_names)
    return df

def drop_table(conn, cursor, table_id):
    cmd = f'DROP TABLE IF EXISTS "{table_id}" CASCADE;'
    execute(conn, cursor, cmd)

class Table:
    """Table class that is a wrapper around Postgres SQL table.

    Parameters
    ----------
    conn : instance of psycopg2.Postgres
        The connection object
    cursor : instance of psycopg2.cursor
        The cursor object
    table_id : str
        The table ID
    column_names : list of str
        The columns to create
    dtypes : list of str
        The datatypes
    primary_key : str | None
        The primary key. If None, the first column name is used
        as primary key.
    foreign_key : dict
        Foreign key referring to another table. The key is the
        name of the foreign key and value is the table it refers to.
    """
    def __init__(self, conn, cursor, table_id, column_names, dtypes,
                 primary_key=None, foreign_key=None):
        self.conn = conn
        self.cursor = cursor
        self.table_id = table_id
        self.column_names = column_names
        if primary_key is None:
            primary_key = column_names[0]
        self.primary_key = primary_key
        if foreign_key is None:
            foreign_key = dict()

        # XXX: add check for columns if table already exists
        create_cmd = f'CREATE TABLE IF NOT EXISTS "{table_id}" ('
    
        if len(column_names) != len(dtypes):
            raise ValueError('Column names and data types should have equal lengths')

        for column_name, dtype in zip(column_names, dtypes):
            create_cmd += f'"{column_name}" {dtype},'
        create_cmd += f'PRIMARY KEY({primary_key}),'
        for key in foreign_key:
            create_cmd += f"""FOREIGN KEY ({key})
                              REFERENCES {foreign_key[key]}({key})
            """
        create_cmd = create_cmd[:-1] + ');'  # remove last comma
        execute(conn, cursor, create_cmd)

    def insert(self, vals, cols=None):
        """Manual insertion into tables

        Parameters
        ----------
        cols : list of str | None
            The columns to insert into. If None, use
            all columns
        vals : list of tuple
            The records to insert. Each tuple
            is one row.
        """
        if cols is None:
            cols = self.column_names
        str_format = ','.join(len(cols) * ['%s'])
        cols = ','.join(cols)
        insert_cmd = f'INSERT INTO {self.table_id}({cols}) VALUES({str_format})'
        _execute_batch(self.conn, self.cursor, insert_cmd, vals)

    def query(self, cmd):
        data = execute(self.conn, self.cursor, cmd, fetch=True)
        df = pd.DataFrame(data, columns=self.column_names)
        df = df.set_index(self.primary_key)
        return df

    def delete(self, condition):
        delete_cmd = f'DELETE FROM {self.table_id} WHERE {condition};'
        execute(self.conn, self.cursor, delete_cmd)

    def drop(self):
        drop_table(self.conn, self.cursor, self.table_id)

connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
               "password='neuroboothrocks'")
csv_fname = ('/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/'
             'register/consent.csv')

conn = psycopg2.connect(connect_str)
cursor = conn.cursor()

df = pd.read_csv(csv_fname)

"""
cols = execute(conn, cursor, get_columns_cmd, fetch=True)
cols = [c for c in cols]
"""

# drop tables if they already exist
# this is just for convenience so we can re-run this script
# even when changing some columns
drop_table(conn, cursor, 'subject')
drop_table(conn, cursor, 'contact')
drop_table(conn, cursor, 'consent')

# Automatic creation of table
table_id = 'consent'
df_to_psql(conn, cursor, df, table_id=table_id)
query = f'SELECT * FROM "{table_id}";'
column_names = df.columns  # XXX: hack
df_consent = psql_to_df(conn, cursor, query, column_names)

# Alternative manual method
table_id = 'subject'
table_subject = Table(conn, cursor, table_id,
                      ['subject_id', 'first_name_birth', 'last_name_birth'],
                      ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)'])
table_subject.insert([('x5dc', 'mainak', 'jas'),
                      ('y5d3', 'anoopum', 'gupta')])
df_subject = table_subject.query(f'SELECT * FROM "{table_id}";')

table_id = 'contact'
table = Table(conn, cursor, table_id,
              column_names=['subject_id', 'email'],
              dtypes=['VARCHAR (255)', 'VARCHAR (255)'],
              foreign_key=dict(subject_id='subject'))
table.insert([('x5dc',), ('y5d3',)], ['subject_id'])
df_contact = table.query(f'SELECT * FROM "{table_id}";')
print(df_contact)

table.delete(condition="subject_id = 'x5dc'")
df_contact = table.query(f'SELECT * FROM "{table_id}";')
print(df_contact)

cursor.close()
conn.close()