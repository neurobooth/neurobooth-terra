# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import pandas as pd

import psycopg2
import psycopg2.extras as extras

#### Some useful PSQL commands
# pg_ctl -D /usr/local/var/postgres start  --> start server
# psql mydatabasename

#### Monkeypatch psycopg2 functions ####

def execute(conn, cursor, cmd, fetch=False):
    cursor.execute(cmd)
    conn.commit()
    if fetch:
        return cursor.fetchall()

def _execute_batch(conn, cursor, cmd, tuples, page_size=100):
    extras.execute_batch(cursor, cmd, tuples, page_size)
    conn.commit()

def _get_primary_key(conn, cursor, table_id):
    query = (
    "SELECT a.attname "
    "FROM   pg_index i "
    "JOIN   pg_attribute a ON a.attrelid = i.indrelid "
                        "AND a.attnum = ANY(i.indkey) "
    f"WHERE  i.indrelid = '{table_id}'::regclass "
    "AND    i.indisprimary;"
    )
    column_names = execute(conn, cursor, query, fetch=True)
    assert len(column_names[0]) == 1  # only one primary key
    return column_names[0][0]

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

def drop_table(table_id, conn):
    cursor = conn.cursor()
    cmd = f'DROP TABLE IF EXISTS "{table_id}" CASCADE;'
    try:
        execute(conn, cursor, cmd)
    except Exception as e:
        cursor.close()
        raise Exception(e)

def create_table(table_id, conn, column_names, dtypes,
                 primary_key=None, foreign_key=None):
    """Create a table.

    Paramters
    ---------
    table_id : str
        The table ID
    conn : instance of psycopg2.Postgres
        The connection object
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
    # XXX: add check for columns if table already exists
    create_cmd = f'CREATE TABLE IF NOT EXISTS "{table_id}" ('

    if len(column_names) != len(dtypes):
        raise ValueError('Column names and data types should have equal lengths')

    if primary_key is None:
        primary_key = column_names[0]
    for column_name, dtype in zip(column_names, dtypes):
        create_cmd += f'"{column_name}" {dtype},'
    create_cmd += f'PRIMARY KEY({primary_key}),'

    if foreign_key is None:
        foreign_key = dict()
    for key in foreign_key:
        create_cmd += f"""FOREIGN KEY ({key})
                            REFERENCES {foreign_key[key]}({key})
        """
    create_cmd = create_cmd[:-1] + ');'  # remove last comma
    cursor = conn.cursor()
    try:
        execute(conn, cursor, create_cmd)
    except Exception as e:
        cursor.close()
        raise Exception(e)
    return Table(table_id, conn=conn, cursor=cursor, primary_key=primary_key)


class Table:
    """Table class that is a wrapper around Postgres SQL table.

    Parameters
    ----------
    table_id : str
        The table ID
    conn : instance of psycopg2.Postgres
        The connection object
    primary_key : str | None
        The primary key. If None, the first column name is used
        as primary key.

    Attributes
    ----------
    column_names : list of str
        The column names
    data_types : list of str
        The data types of the column names
    """
    def __init__(self, table_id, conn, cursor=None,
                 primary_key=None):
        self.conn = conn
        if cursor is None:
            cursor = conn.cursor()
        self.cursor = cursor
        self.table_id = table_id

        alias = {'character varying': 'VARCHAR'}
        cmd = ("SELECT column_name, data_type, character_maximum_length"
               " FROM INFORMATION_SCHEMA.COLUMNS WHERE "
               f"table_name = '{table_id}';")
        columns = execute(conn, cursor, cmd, fetch=True)

        self.column_names = list()
        self.data_types = list()
        for cn in columns:
            column_name, dtype, maxlen = cn
            if dtype == 'character varying':
                dtype = f'VARCHAR ({maxlen})'
            self.column_names.append(column_name)
            self.data_types.append(dtype.upper())

        if primary_key is None:
            primary_key = _get_primary_key(conn, cursor, table_id)
        self.primary_key = primary_key

    def __repr__(self):
        repr_str = f'Table "{self.table_id}" '
        repr_str += '(' + ', '.join(self.column_names) + ')'
        return repr_str

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def close(self):
        self.cursor.close()

    def alter_column(self, col, default=None):
        """Alter a column in the table.

        Parameters
        ----------
        col : str
            The column name.
        default : str | dict | None
            The default value of the column.
            If you want to specify a prefix that
            autoincrements, you can say:
            dict(prefix=prefix), e.g.,
            dict(prefix='SUBJECT')
        """
        cmd = f"ALTER TABLE {self.table_id} ALTER COLUMN {col} "
        if isinstance(default, str):
            cmd += f'SET DEFAULT {default}'
            execute(self.conn, self.cursor, cmd)
        elif isinstance(default, dict):
            sequence_name = f'{self.table_id}_{col}'
            seq_cmd1 = f'DROP SEQUENCE IF EXISTS {sequence_name}'
            seq_cmd2 = f'CREATE SEQUENCE  IF NOT EXISTS {sequence_name}'
            execute(self.conn, self.cursor, seq_cmd1)
            execute(self.conn, self.cursor, seq_cmd2)

            prefix = default['prefix']
            cmd += f"SET DEFAULT '{prefix}' || nextval('{sequence_name}')"
            execute(self.conn, self.cursor, cmd)

            constraint_name = f'{sequence_name}_chk'
            check_cmd = (f"ALTER TABLE {self.table_id} "
                         f"ADD CONSTRAINT {constraint_name} "
                         f"CHECK ({col} ~ '^{prefix}[0-9]+$')")
            execute(self.conn, self.cursor, check_cmd)

    def add_column(self, col, dtype):
        """Add a new column to the table.

        Parameters
        ----------
        col : str
            The column name.
        dtype : str
            The data type of the column.
        """
        cmd = f'ALTER TABLE {self.table_id} '
        cmd += f'ADD COLUMN {col} {dtype};'
        execute(self.conn, self.cursor, cmd)
        self.column_names.append(col)

    def drop_column(self, col):
        """Drop a column from the table.

        Parameters
        ----------
        col : str
            The column name.
        """
        cmd = f'ALTER TABLE {self.table_id} '
        cmd += f'DROP COLUMN {col} '
        execute(self.conn, self.cursor, cmd)

        idx = self.column_names.index(col)
        del self.column_names[idx], self.data_types[idx]

    def insert_rows(self, vals, cols):
        """Manual insertion into tables

        Parameters
        ----------
        vals : list of tuple
            The records to insert. Each tuple
            is one row.
        cols : list of str
            The columns to insert into.

        Returns
        -------
        pk_val : str | None
            The primary keys of the row inserted into.
            If multiple rows are inserted, returns None.
        """
        if not isinstance(vals, list):
            raise ValueError(f'vals must be a list of tuple. Got {type(vals)}')
        for val in vals:
            if not isinstance(val, tuple):
                raise ValueError(f'entries in vals must be tuples. Got {type(val)}')
            if len(val) != len(cols):
                raise ValueError(f'tuple length must match number of columns ({len(cols)})')
        str_format = ','.join(len(cols) * ['%s'])
        cols = ','.join([f'"{col}"' for col in cols])
        insert_cmd = f'INSERT INTO {self.table_id}({cols}) VALUES({str_format}) '
        insert_cmd += f'RETURNING {self.primary_key}'

        _execute_batch(self.conn, self.cursor, insert_cmd, vals)
        if len(vals) == 1:
            return self.cursor.fetchone()[0]

    def update_row(self, pk_val, vals, cols=None):
        """Update values in a row

        Parameters
        ----------
        pk_val : str
            The value of the primary key to match
            the row to replace.
        vals : tuple
            The values in the row to replace.
        cols : list of str | None
            The columns to insert into. If None, use
            all columns
        """
        cmd = f"UPDATE {self.table_id} SET "

        if not isinstance(vals, tuple):
            raise ValueError('vals must be a tuple')

        if cols is None:
            cols = self.column_names

        if len(cols) != len(vals):
            raise ValueError(f'length of vals ({len(vals)}) != '
                             f'length of cols ({len(cols)})')

        for col, val in zip(cols, vals):
            if col not in self.column_names:
                raise ValueError(f'column {col} is not present in table')
            cmd += f"\"{col}\" = '{val}', "
        cmd = cmd[:-2]  # remove last comma
        cmd += f" WHERE {self.primary_key} = '{pk_val}';"
        execute(self.conn, self.cursor, cmd)

    def query(self, cmd):
        """Run a query.

        Returns
        -------
        df : instance of pd.Dataframe
            A pandas dataframe object.
        """
        data = execute(self.conn, self.cursor, cmd, fetch=True)
        df = pd.DataFrame(data, columns=self.column_names)
        df = df.set_index(self.primary_key)
        return df

    def delete_row(self, condition):
        delete_cmd = f'DELETE FROM {self.table_id} WHERE {condition};'
        execute(self.conn, self.cursor, delete_cmd)

    def drop(self):
        drop_table(self.conn, self.cursor, self.table_id)


def list_tables(conn):
    """List the table_ids in the database.

    Parameters
    ----------
    conn : instance of psycopg2.Postgres
        The connection object

    Returns
    -------
    table_ids : list of str
        The table IDs
    """
        
    query_tables_cmd = """
    SELECT *
    FROM pg_catalog.pg_tables
    WHERE schemaname != 'pg_catalog' AND 
        schemaname != 'information_schema';
    """
    cursor = conn.cursor()
    cursor.execute(query_tables_cmd)

    tables = cursor.fetchall()
    cursor.close()

    table_ids = [table[1] for table in tables]
    return table_ids
