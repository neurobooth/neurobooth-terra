import os
import shutil
from tempfile import TemporaryDirectory, NamedTemporaryFile, mkdtemp

import pytest
import psycopg2

from neurobooth_terra import create_table, drop_table, Table
from neurobooth_terra.dataflow import write_file, transfer_files, delete_files

db_args = dict(database='neurobooth', user='neuroboother',
               password='neuroboothrocks')


@pytest.fixture(scope="session")
def mock_data():
    """Create mock tables temporarily for testing purposes."""

    src_dirname = os.path.join(mkdtemp(), '')  # ensure path ends with /
    dest_dirname = mkdtemp()

    os.mkdir(os.path.join(src_dirname, 'test_subfolder'))

    with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
        table_id = 'log_sensor_file'
        column_names = ['log_sensor_file_id', 'sensor_file_path']
        dtypes = ['VARCHAR (255)', 'text[]']
        drop_table(table_id, conn)
        create_table(table_id, conn, column_names, dtypes,
                     primary_key='log_sensor_file_id')

        table_id = 'log_file'
        column_names = ['operation_id', 'log_sensor_file_id', 'src_dirname',
                        'dest_dirname', 'fname', 'time_copied',
                        'rsync_operation', 'is_deleted']
        dtypes = ['SERIAL', 'text', 'text',
                  'text', 'text', 'timestamp', 'text',
                  'boolean']
        drop_table(table_id, conn)
        create_table(table_id, conn, column_names, dtypes,
                     primary_key='operation_id',
                     foreign_key={'log_sensor_file_id': 'log_sensor_file'})

    yield src_dirname, dest_dirname

    # cleanup
    shutil.rmtree(src_dirname)
    shutil.rmtree(dest_dirname)


def test_write(mock_data):
    """Test writing files."""
    src_dirname, _ = mock_data

    with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
        sensor_file_table = Table('log_sensor_file', conn)
        db_table = Table('log_file', conn)

        for id in range(5):
            with NamedTemporaryFile(dir=src_dirname, delete=False) as fp:
                fp.write(b'Hello world!')
                dest_dir, fname = os.path.split(fp.name)
                write_file(sensor_file_table, db_table, dest_dir, fname, id)

        # Write file to a subfolder
        dest_dir2 = os.path.join(src_dirname, 'test_subfolder')
        with NamedTemporaryFile(dir=dest_dir2, delete=False) as fp:
            fp.write(b'test')
            _, fname = os.path.split(fp.name)
            fname = os.path.join('test_subfolder', fname)
            write_file(sensor_file_table, db_table, dest_dir, fname, id=5)

        with pytest.raises(ValueError, match='does not exist'):
            write_file(sensor_file_table, db_table, dest_dir, fname='blah',
                       id=6)


def test_transfer(mock_data):
    """Test transfer of files."""
    src_dirname, dest_dirname = mock_data
    with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
        db_table = Table('log_file', conn)
        sensor_file_table = Table('log_sensor_file', conn)
        db_rows = transfer_files(src_dirname, dest_dirname, db_table,
                                 sensor_file_table)


def test_delete(mock_data):
    """Test deleting files."""
    src_dirname, dest_dirname = mock_data
    with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
        db_table = Table('log_file', conn)
        delete_files(db_table, target_dir=src_dirname,
                     suitable_dest_dir=dest_dirname,
                     threshold=0.1, older_than=-1)
