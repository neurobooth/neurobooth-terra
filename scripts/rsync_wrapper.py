import os
import shutil
from tempfile import TemporaryDirectory, NamedTemporaryFile, mkdtemp

import psycopg2

from neurobooth_terra import create_table, drop_table, Table
from neurobooth_terra.dataflow import write_file, transfer_files, delete_files

db_args = dict(database='neurobooth', user='neuroboother',
               password='neuroboothrocks')

# Create tables temporarily for testing purposes
with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
    table_id = 'log_sensor_file'
    column_names = ['log_sensor_file_id', 'sensor_file_path']
    dtypes = ['VARCHAR (255)', 'text[]']
    drop_table(table_id, conn)
    create_table(table_id, conn, column_names, dtypes,
                 primary_key='log_sensor_file_id')

    table_id = 'log_file'
    column_names = ['operation_id', 'log_sensor_file_id', 'src_dirname',
                    'dest_dirname', 'fname', 'time_copied', 'rsync_operation',
                    'is_deleted']
    dtypes = ['SERIAL', 'text', 'text',
              'text', 'text', 'timestamp', 'text',
              'boolean']
    drop_table(table_id, conn)
    create_table(table_id, conn, column_names, dtypes,
                 primary_key='operation_id',
                 foreign_key={'log_sensor_file_id': 'log_sensor_file'})


src_dirname = os.path.join(mkdtemp(), '')  # ensure path ends with /
dest_dirname = mkdtemp()

for id in range(5):
    with NamedTemporaryFile(dir=src_dirname, delete=False) as fp:
        fp.write(b'Hello world!')
        # Adonay would need this in his code.
        with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
            sensor_file_table = Table('log_sensor_file', conn)
            db_table = Table('log_file', conn)
            write_file(sensor_file_table, db_table, fp.name, id)

# This would be a separate script
with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
    db_table = Table('log_file', conn)
    db_rows = transfer_files(src_dirname, dest_dirname, db_table,
                             sensor_file_table)
    delete_files(db_table, target_dir=src_dirname,
                 suitable_dest_dir=dest_dirname,
                 threshold=0.1, older_than=-1)

# cleanup
shutil.rmtree(src_dirname)
shutil.rmtree(dest_dirname)
