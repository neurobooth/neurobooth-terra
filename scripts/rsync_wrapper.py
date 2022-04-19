"""Utility functions for file transfers."""

# Author: Mainak Jas <mainakjas@gmail.com>

import os
import datetime
import shutil
import subprocess
import warnings
from tempfile import TemporaryDirectory, NamedTemporaryFile

import psycopg2

from neurobooth_terra import create_table, drop_table, Table


def write_file(sensor_file_table, db_table, fname, id=0):
    """Write a file.

    Parameters
    ----------
    sensor_file_table : instance of Table
        The table containing information about the sensors used in a session
        and the files.
    db_table : instance of Table
        The table containing information about the file transfers.
    fname : str
        The filename to write.
    id : int
        The row number of sensor_file_table to be used as primary key.
    """
    dir, fname = os.path.split(fp.name)
    column_names = ['sensor_file_id', 'sensor_file_path']
    sensor_file_table.insert_rows(
        [(f'sensor_{id}', [fname])], cols=column_names)

    time_copied = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    column_names = ['sensor_file_id', 'src_dirname', 'fname',
                    'time_copied', 'rsync_operation', 'is_deleted']
    db_table.insert_rows([(f'sensor_{id}', dir, fname, time_copied, None, False)],
                         cols=column_names)


def transfer_files(src_dir, dest_dir, db_table, sensor_file_table):
    """Transfer files using rsync."""
    out = subprocess.run(["rsync", src_dirname, dest_dirname, '-arzi',
                          "--out-format=%i %n%L %t"],
                         capture_output=True)
    if len(out.stderr) > 0:
        warnings.warn(out.stderr)

    out = out.stdout.decode('ascii').split('\n')

    # what happens if it exits midway?
    # check hashes before writing to table.
    column_names = ['sensor_file_id', 'src_dirname', 'dest_dirname', 'fname',
                    'time_copied', 'rsync_operation', 'is_deleted']
    db_rows = list()
    for this_out in out:
        if this_out.startswith('>f'):
            operation, fname, date_copied, time_copied = this_out.split(' ')
            _, fname = os.path.split(fname)
            df = sensor_file_table.query(
                where=f"sensor_file_path @> ARRAY['{fname}']").reset_index()
            sensor_file_id = df.sensor_file_id[0]
            db_rows.append((sensor_file_id, src_dirname, dest_dirname,
                            fname, f'{date_copied}_{time_copied}', operation,
                            False))

    db_table.insert_rows(db_rows, column_names)
    return db_rows


def delete_files(db_table, src_dir, suitable_dest_dir=None, threshold=0.9,
                 older_than=30):
    """Delete files if x% of disk is filled.

    Parameters
    ----------
    db_table : instance of Table
        The database table containing information about file transfers. The
        row corresponding to the transferred file is updated with
        is_deleted=True.
    src_dir : str
        The source directory path from which to delete files.
    suitable_dest_dir : str | None
        The destination directory path where the files should have been copied.
        If None, delete files regardless of which directory they have been
        copied to.
    threshold : float
        A fraction between 0. and 1. If more than threshold fraction of the
        source directory is filled, these files will be deleted.
    older_than : int
        If a file is older than older_than days in both src_dir and
        suitable_dest_dir, they will be deleted.
    """
    stats = shutil.disk_usage(src_dir)
    if stats.used / stats.total < threshold:
        return

    where = "DATE_PART('day', AGE(current_timestamp, time_copied)) < 30 "
    where += "AND is_deleted=False "
    where += f"AND src_dirname='{src_dir}' "

    where_transferred = where
    if suitable_dest_dir is not None:
        where_transferred = where + f"AND dest_dirname='{suitable_dest_dir}'"
    else:
        where_transferred = where + "AND dest_dirname IS NOT NULL"
    transferred_files_df = db_table.query(where=where_transferred)

    where_untransferred = where + f"AND dest_dirname IS NULL"
    untransferred_files_df = db_table.query(where=where_untransferred)
    if len(untransferred_files_df) > 0:
        print(f'The following files are older than {older_than} days but not '
              f'transferred to {suitable_dest_dir} yet')
        print(untransferred_files_df)

    for operation_id, files_row in transferred_files_df.iterrows():
        if suitable_dest_dir is not None:
            assert suitable_dest_dir == files_row.dest_dirname

        fname = os.path.join(src_dir, files_row.fname)
        try:
            os.remove(fname)
        except Exception as e:
            print(f'Deleting ...\n')
            print(files_row)
            raise(e)
        # XXX: might be a little inefficient to loop file-by-file but
        # probably more robust in case of failure.
        db_table.insert_rows([(operation_id, True,)],
                             cols=['operation_id', 'is_deleted'],
                             on_conflict='update')


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
                    'dest_dirname', 'fname', 'time_copied', 'rsync_operation',
                    'is_deleted']
    dtypes = ['SERIAL', 'text', 'text',
              'text', 'text', 'timestamp', 'text',
              'boolean']
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
            delete_files(db_table, src_dirname, suitable_dest_dir=None,
                         threshold=0.1)
