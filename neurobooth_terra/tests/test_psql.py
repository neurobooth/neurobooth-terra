import psycopg2

import pytest
from numpy.testing import assert_raises

from neurobooth_terra import Table, create_table, drop_table


def test_psql_connection():
    """Test that we can connect to the database"""

    connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
                   "password='neuroboothrocks'")

    conn = psycopg2.connect(connect_str)

    table_id = 'test'
    drop_table(table_id, conn)

    column_names = ['subject_id', 'first_name_birth', 'last_name_birth', 'age']
    dtypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)', 'INTEGER']
    table_subject = create_table(table_id, conn=conn,
                                 column_names=column_names,
                                 dtypes=dtypes)
    table_subject.insert_rows([('x5dc', 'mainak', 'jas', 21),
                               ('y5d3', 'anoopum', 'gupta', 25)])
    with pytest.raises(ValueError, match='vals must be a list of tuple'):
        table_subject.insert_rows('blah')
    with pytest.raises(ValueError, match='entries in vals must be tuples'):
        table_subject.insert_rows(['blah'])
    with pytest.raises(ValueError, match='tuple length must match'):
        table_subject.insert_rows([('x5dc', 'mainak', 'jas')])

    assert table_subject.data_types == dtypes
    table_subject.close()

    table_test = Table('test', conn)
    assert table_test.primary_key == 'subject_id'
    conn.close()
