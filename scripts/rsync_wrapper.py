"""Utility functions for file transfers."""

# Author: Mainak Jas <mainakjas@gmail.com>

import os
import datetime
import shutil
import subprocess
import warnings
from tempfile import TemporaryDirectory, NamedTemporaryFile, mkdtemp

import psycopg2
import pandas as pd

from neurobooth_terra import create_table, drop_table, Table


def _do_files_match(src_dirname, dest_dirname, fname):
    """Compare two files using a hash."""

    # we could also generate hash in Python but reading the file in Python
    # may be more memory intense.
    out_src = subprocess.run(["shasum", os.path.join(src_dirname, fname)],
                             capture_output=True).stdout.decode('ascii')
    out_dest = subprocess.run(["shasum", os.path.join(dest_dirname, fname)],
                              capture_output=True).stdout.decode('ascii')

    hash_src, hash_dest = out_src.split(' ')[0], out_dest.split(' ')[0]
    if hash_src != hash_dest:  # could be partially copied?
        print(f'hash of file {fname} does not match: '
              f'({hash_src}, {hash_dest})')
        return False
    return True


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
    dir = os.path.join(dir, '')  # ensure trailing slash
    column_names = ['log_sensor_file_id', 'sensor_file_path']
    sensor_file_table.insert_rows(
        [(f'sensor_{id}', [fname])], cols=column_names)

    time_copied = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    column_names = ['log_sensor_file_id', 'src_dirname', 'fname',
                    'dest_dirname', 'time_copied', 'rsync_operation',
                    'is_deleted']
    db_table.insert_rows([(f'sensor_{id}', None, fname,
                           dir, time_copied, None, False)],
                         cols=column_names)


def transfer_files(src_dir, dest_dir, db_table, sensor_file_table):
    """Transfer files using rsync."""
    out = subprocess.run(["rsync", src_dir, dest_dir, '-arzi',
                          "--out-format=%i %n%L %t"],
                         capture_output=True)
    if len(out.stderr) > 0:
        warnings.warn(out.stderr)

    out = out.stdout.decode('ascii').split('\n')

    # >f tells us that a file will be transferred but it does not tell us
    # if rsync did actually manage to finish the transfer. Therefore, we
    # will run manually check the hashes of the files before writing to the
    # table.
    column_names = ['log_sensor_file_id', 'src_dirname', 'dest_dirname', 'fname',
                    'time_copied', 'rsync_operation', 'is_deleted']
    db_rows = list()
    for this_out in out:
        if this_out.startswith('>f'):
            operation, fname, date_copied, time_copied = this_out.split(' ')
            # only write to table if files match with hash
            if not _do_files_match(src_dir, dest_dir, fname):
                continue

            df = sensor_file_table.query(
                where=f"sensor_file_path @> ARRAY['{fname}']").reset_index()
            log_sensor_file_id = df.log_sensor_file_id[0]

            # ensure trailing slash (but don't provide to rsync)
            dest_dir = os.path.join(dest_dir, '')
            src_dir = os.path.join(src_dir, '')
            db_rows.append((log_sensor_file_id, src_dir, dest_dir,
                            fname, f'{date_copied}_{time_copied}', operation,
                            False))

    db_table.insert_rows(db_rows, column_names)
    return db_rows


def delete_files(db_table, target_dir, suitable_dest_dir, threshold=0.9,
                 older_than=30):
    """Delete files if x% of disk is filled.

    Parameters
    ----------
    db_table : instance of Table
        The database table containing information about file transfers. The
        row corresponding to the transferred file is updated with
        is_deleted=True.
    target_dir : str
        The source directory path from which to delete files.
    suitable_dest_dir : str
        The destination directory path where the files should have been copied.
    threshold : float
        A fraction between 0. and 1. If more than threshold fraction of the
        source directory is filled, these files will be deleted.
    older_than : int
        If a file is older than older_than days in both src_dir and
        suitable_dest_dir, they will be deleted.

    Notes
    -----
    Let's say the data was first written to "Nas", then copied to "who"
    and from "who" to "neo"

    src       dest    is_deleted
    ----------------------------
    Null      Nas     False
    Nas       who     False
    who       neo     False

    Now, if we want to delete the file from "Nas" and ensure that it has
    been copied to the *final* destination neo, we provide the following
    arguments to the function:

      target_dir = Nas, suitable_dest = neo

    Null      Nas     True
    Nas       who     False
    who       neo     False

    Note that is_deleted bool is set to True where dest = target_dir

    Here is another example
      target_dir = who, suitable_dest = neo

    Null      Nas     True
    Nas       who     True
    who       neo     False
    """
    # ensure trailing slash
    target_dir = os.path.join(target_dir, '')
    suitable_dest_dir = os.path.join(suitable_dest_dir, '')

    stats = shutil.disk_usage(target_dir)
    if stats.used / stats.total < threshold:
        return

    where = f"dest_dirname='{target_dir}' "
    where += f"AND DATE_PART('day', AGE(current_timestamp, time_copied)) > {older_than} "
    where += "AND is_deleted=False"
    fnames_to_delete = db_table.query(where=where).fname

    if len(fnames_to_delete) == 0:
        print('No files to delete')
        return

    fnames = [f"\'{fname}\'" for fname in fnames_to_delete.tolist()]
    fnames = ', '.join(fnames)
    where = f"dest_dirname!='{suitable_dest_dir}' "
    where += f"AND src_dirname IS NOT NULL "  # exclude write operations
    where += "AND is_deleted=False "  # just to be safe
    where += f"AND fname IN ({fnames})"
    fnames_not_transferred = db_table.query(where=where).fname.tolist()
    if len(fnames_not_transferred) > 0:
        print(f'The files {fnames_not_transferred} have been in target_dir '
              f'for more than {older_than} days but have not been transferred '
              f'to suitable_dest_dir={suitable_dest_dir}')
    # TODO: output files that are in target_dir but not in log_file table
    # (e.g., research coordinator notes). Might require function to walk
    # over sub-folders.

    where = where.replace("dest_dirname!=", "dest_dirname=")
    fnames_transferred = db_table.query(where=where).fname.tolist()
    fnames_transferred = [f"\'{fname}\'" for fname in fnames_transferred]
    fnames_transferred = ', '.join(fnames_transferred)
    if len(fnames_transferred) == 0:
        raise ValueError('No files have been transferred to suitable_dest_dir')

    where = f"dest_dirname='{target_dir}' "
    where += f"AND fname IN ({fnames_transferred})"
    files_transferred_df = db_table.query(where=where)

    for operation_id, files_row in files_transferred_df.iterrows():

        fname = os.path.join(target_dir, files_row.fname)
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
