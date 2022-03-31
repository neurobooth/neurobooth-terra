import os
import subprocess
from tempfile import TemporaryDirectory, NamedTemporaryFile

import psycopg2

from neurobooth_terra import create_table, drop_table, Table


def write_file(sensor_file_table, db_table, fname, id=0):
    """Write a file."""
    dir, fname = os.path.split(fp.name)
    column_names = ['sensor_file_id', 'sensor_file_path']
    sensor_file_table.insert_rows(
        [(f'sensor_{id}', [fname])], cols=column_names)

    column_names = ['src_dirname', 'dest_dirname', 'fname', 'rsync_operation']
    db_table.insert_rows([(None, dir, fname, None)], cols=column_names)


def transfer_files(src_dir, dest_dir, db_table, sensor_file_table):
    """Transfer files using rsync."""
    out = subprocess.run(["rsync", src_dirname, dest_dirname, '-arzi'],
                         capture_output=True)
    if len(out.stderr) > 0:
        raise ValueError(out.stderr)

    out = out.stdout.decode('ascii').split('\n')

    column_names = ['sensor_file_id', 'src_dirname', 'dest_dirname', 'fname',
                    'rsync_operation']
    db_rows = list()
    for this_out in out:
        if this_out.startswith('>f'):
            operation, fname = this_out.split(' ')
            _, fname = os.path.split(fname)
            df = sensor_file_table.query(
                where=f"sensor_file_path @> ARRAY['{fname}']").reset_index()
            sensor_file_id = df.sensor_file_id[0]
            db_rows.append((sensor_file_id, src_dirname, dest_dirname,
                            fname, operation))

    db_table.insert_rows(db_rows, column_names)
    return db_rows


def delete_files(src_dir, threshold=0.9):
    """Delete files if x% of disk is filled."""
    import shutil
    stats = shutil.disk_usage(src_dir)
    if stats.used / stats.total < 0.9:
        return
    # shutil.rmtree


db_args = dict(database='neurobooth', user='neuroboother',
               password='neuroboothrocks')

# Create tables temporarily for testing purposes
with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
    table_id = 'sensor_file_log'
    column_names = ['sensor_file_id', 'sensor_file_path']
    dtypes = ['VARCHAR (255)', 'text[]']
    drop_table(table_id, conn)
    create_table(table_id, conn, column_names, dtypes,
                 primary_key='sensor_file_id')

    table_id = 'file'
    column_names = ['operation_id', 'sensor_file_id', 'src_dirname',
                    'dest_dirname', 'fname', 'rsync_operation']
    dtypes = ['text'] * len(column_names)
    dtypes[0] = 'SERIAL'
    drop_table(table_id, conn)
    create_table(table_id, conn, column_names, dtypes,
                 primary_key='operation_id',
                 foreign_key={'sensor_file_id': 'sensor_file_log'})


with TemporaryDirectory() as src_dirname:
    with TemporaryDirectory() as dest_dirname:
        for id in range(5):
            with NamedTemporaryFile(dir=src_dirname, delete=False) as fp:
                fp.write(b'Hello world!')

                # Adonay would need this in his code.
                with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
                    sensor_file_table = Table('sensor_file_log', conn)
                    db_table = Table('file', conn)
                    write_file(sensor_file_table, db_table, fp.name, id)

        # This would be a separate script
        with psycopg2.connect(port='5432', host='localhost', **db_args) as conn:
            db_table = Table('file', conn)
            db_rows = transfer_files(src_dirname, dest_dirname, db_table,
                                     sensor_file_table)

            # delete_files(src_dirname)
