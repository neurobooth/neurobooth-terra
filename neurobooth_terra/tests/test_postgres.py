import datetime

import psycopg2

import pytest
from numpy.testing import assert_raises

from neurobooth_terra import Table, create_table, drop_table, query


connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
                "password='neuroboothrocks'")

def test_psql_connection():
    """Test that we can connect to the database"""

    conn = psycopg2.connect(connect_str)

    table_id = 'test'
    drop_table(table_id, conn)

    column_names = ['subject_id', 'first_name_birth', 'last_name_birth', 'Age']
    dtypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)', 'INTEGER']
    table_subject = create_table(table_id, conn=conn,
                                 column_names=column_names,
                                 dtypes=dtypes)
    table_subject.insert_rows([('x5dc', 'mainak', 'jas', 21),
                               ('y5d3', 'anoopum', 'gupta', 25)],
                               cols=column_names)
    with pytest.raises(ValueError, match='vals must be a list of tuple'):
        table_subject.insert_rows('blah', ['subject_id'])
    with pytest.raises(ValueError, match='entries in vals must be tuples'):
        table_subject.insert_rows(['blah'], ['subject_id'])
    with pytest.raises(ValueError, match='tuple length must match'):
        table_subject.insert_rows([('x5dc', 'mainak', 'jas')], ['subject_id'])

    assert sorted(table_subject.data_types) == sorted(dtypes)
    table_subject.close()

    table_test = Table('test', conn)
    assert table_test.primary_key == ['subject_id']

    # test updating row
    table_test.update_row('y5d3', ('blah', 'anupum', 'gupta', 32),
                          cols=column_names)
    df = table_test.query()
    assert 'blah' in df.index

    with pytest.raises(ValueError, match='vals must be a tuple'):
        table_test.update_row('blah', 'mainak', ['first_name_birth'])

    # test updating row partially
    table_test.update_row('blah', ('mainak',), ['first_name_birth'])
    df = table_test.query()
    assert df.loc['blah']['first_name_birth'] == 'mainak'

    with pytest.raises(ValueError, match='column blah is not present'):
        table_test.update_row('blah', ('mainak',), ['blah'])

    with pytest.raises(ValueError, match='length of vals'):
        table_test.update_row('blah', ('mainak',),
                              ['first_name_birth', 'last_name_birth'])

    # test dropping a column
    table_test.drop_column(col='subject_id')
    assert 'subject_id' not in table_test.column_names

    # test adding an auto-incrementing default value to the column
    table_test.add_column(col='subject_id', dtype='VARCHAR')
    table_test.alter_column(col='subject_id', default=dict(prefix='SUBJ'))
    pk_val = table_test.insert_rows([('mainak', 'jas', 21)],
                                    cols=['first_name_birth', 'last_name_birth', 'Age'])
    assert pk_val == 'SUBJ1'
    df = table_test.query()
    assert 'SUBJ1' in df.index

    # test insertion of date
    table_id = 'test_consent'
    drop_table(table_id, conn)

    column_names = ['subject_id', 'consent_date']
    dtypes = ['VARCHAR (255)', 'date']
    table_consent = create_table(table_id, conn, column_names, dtypes)
    date = datetime.datetime.today().strftime('%Y-%m-%d')
    table_consent.insert_rows([('x5dc', date)], cols=column_names)

    conn.close()


def test_compound_primary_key():
    """Test compound primary key."""
    conn = psycopg2.connect(connect_str)

    table_id = 'test'
    drop_table(table_id, conn)

    column_names = ['subject_id', 'first_name_birth', 'last_name_birth', 'Age']
    dtypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)', 'INTEGER']
    primary_key = ['first_name_birth', 'last_name_birth']
    table_subject = create_table(table_id, conn=conn,
                                 column_names=column_names,
                                 dtypes=dtypes,
                                 primary_key=primary_key)
    assert set(table_subject.primary_key) == set(primary_key)


def test_delete():
    """Test deleting rows"""

    conn = psycopg2.connect(connect_str)

    table_id = 'test'
    drop_table(table_id, conn)

    column_names = ['subject_id', 'first_name_birth', 'last_name_birth', 'Age']
    dtypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)', 'INTEGER']
    table_subject = create_table(table_id, conn=conn,
                                 column_names=column_names,
                                 dtypes=dtypes)
    table_subject.insert_rows([('x5dc', 'mainak', 'jas', 21),
                               ('y5d3', 'anoopum', 'gupta', 25),
                               ('abcd', 'mayank', 'jas', 25)],
                               cols=column_names)
    table_subject.delete_row("first_name_birth LIKE 'ma%'")
    df = table_subject.query()
    assert len(df['first_name_birth']) == 1


def test_upsert():
    """Test upsert operation."""

    conn = psycopg2.connect(connect_str)

    table_id = 'test'
    drop_table(table_id, conn)

    column_names = ['subject_id', 'first_name_birth', 'last_name_birth', 'Age',
                    'attributes']
    dtypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)', 'INTEGER',
              'JSONB']
    table_subject = create_table(table_id, conn=conn,
                                 column_names=column_names,
                                 dtypes=dtypes)
    table_subject.insert_rows([('x5dc', 'mainak', 'jas', 21, {'a': 1}),
                               ('y5d3', 'anoopum', 'gupta', 25, {'b': 2}),
                               ('abcd', 'mayank', 'jas', 25, {'a': 1})],
                               cols=column_names)
    table_subject.insert_rows([('x5dc', 'mainak_new', 'jas_new', 21, {'a': 1}),
                               ('zzzz', 'deepak', 'singh', 32, {'d': 1})],
                               cols=column_names, on_conflict='nothing')
    df = table_subject.query()
    assert 'x5dc' in df.index
    assert 'zzzz' in df.index
    assert df.loc['x5dc']['first_name_birth'] == 'mainak'  # not mainak_new

    df = table_subject.query(where="subject_id = 'x5dc'")
    assert 'x5dc' in df.index
    assert 'zzzz' not in df.index

    # smoke test include_columns
    df = table_subject.query(include_columns='first_name_birth',
                             where="subject_id = 'x5dc'")
    df = table_subject.query(include_columns=['first_name_birth'],
                             where="subject_id = 'x5dc'")

    df = query(conn,
               "SELECT first_name_birth FROM test where subject_id = 'x5dc' ",
               column_names='first_name_birth')

    table_subject.insert_rows([('x5dc', 'mainak', 'jazz', 32, {'a': 1})],
                               cols=column_names, on_conflict='update')
    df = table_subject.query(where="subject_id = 'x5dc'")
    assert df.loc['x5dc']['last_name_birth'] == 'jazz'
