import psycopg2
import pytest

from neurobooth_terra import Table, create_table, drop_table


def test_psql_connection():
    """Test that we can connect to the database"""

    connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
                   "password='neuroboothrocks'")

    conn = psycopg2.connect(connect_str)

    table_id = 'test'
    drop_table(table_id, conn)
    table_subject = create_table(table_id, conn=conn,
                                 column_names=['subject_id', 'first_name_birth', 'last_name_birth'],
                                 dtypes=['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)'])
    table_subject.insert_rows([('x5dc', 'mainak', 'jas'),
                               ('y5d3', 'anoopum', 'gupta')])
    table_subject.close()
    conn.close()
