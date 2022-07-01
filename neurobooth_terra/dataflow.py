"""Utility functions for file transfers."""

# Author: Mainak Jas <mainakjas@gmail.com>

import os
from pydoc import source_synopsis
import shutil
import time
import datetime
import subprocess
import warnings

import pandas as pd


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


def write_files(sensor_file_table, db_table, dest_dir):
    """Write a file to log_file table.

    Parameters
    ----------
    sensor_file_table : instance of Table
        The table containing information about the sensors used in a session
        and the files.
    db_table : instance of Table
        The table containing information about the file transfers.
    dest_dir : str
        The destination directory.
    """
    _, session_name = os.path.split(dest_dir)
    dest_dir = os.path.join(dest_dir, '')  # ensure trailing slash

    log_file_df = db_table.query(where=f"dest_dirname='{dest_dir}'")
    sensor_file_df = sensor_file_table.query()

    # assuming one sensor_file per row
    sensor_fnames_df = sensor_file_df.sensor_file_path.tolist()
    sensor_file_ids = sensor_file_df.index.tolist()
    sensor_fnames = list()
    for sensor_fname_row, sensor_file_id in zip(sensor_fnames_df, sensor_file_ids):
        for this_sensor_fname in sensor_fname_row:
            _, this_sensor_fname = os.path.split(this_sensor_fname)
            if session_name in this_sensor_fname:
                sensor_fnames.append((sensor_file_id, this_sensor_fname))

    missing_fnames = [(sensor_file_id, fname)
                      for sensor_file_id, fname in sensor_fnames
                      if fname not in log_file_df.fname.tolist()]

    column_names = ['log_sensor_file_id', 'src_dirname', 'fname',
                    'dest_dirname', 'time_verified', 'rsync_operation',
                    'is_deleted']
    for sensor_file_id, fname in missing_fnames:
        if os.path.exists(os.path.join(dest_dir, fname)):
            time_verified = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            db_table.insert_rows([(sensor_file_id, None, fname,
                                 dest_dir, time_verified, None, False)],
                                 cols=column_names)
        else:
            print(f'{fname} exists in log_sensor_file table, but does not exist in {dest_dir}')


def _update_copystatus(db_table, show_unfinished=False):
    """Update copy status after checking if files match"""
    log_file_df = db_table.query(where='is_finished=False')
    for operation_id, log_file_row in log_file_df.iterrows():
        if _do_files_match(log_file_row['src_dirname'],
                           log_file_row['dest_dirname'],
                           log_file_row['fname']):
            db_table.insert_rows([(operation_id, True,)],
                                 ['operation_id', 'is_finished'],
                                 on_conflict='update')
        elif show_unfinished:
            db_table.delete_row(where=f"operation_id={operation_id}")
            print(f"The file transfer from {log_file_row['src_dirname']} "
                  f"to {log_file_row['dest_dirname']} did not finish for "
                  f"file {log_file_row['fname']}")


def copy_files(src_dir, dest_dir, db_table, sensor_file_table):
    """Transfer files using rsync."""

    _update_copystatus(db_table, show_unfinished=True)
    out = subprocess.run(["rsync", src_dir, dest_dir, '-a', '--dry-run',
                          "--out-format=%i %n%L %t"],
                         capture_output=True)
    if len(out.stderr) > 0:
        warnings.warn(out.stderr.decode('ascii'))

    out = out.stdout.decode('ascii').split('\n')


    # >f tells us that a file will be transferred but it does not tell us
    # if rsync did actually manage to finish the transfer. Therefore, we
    # will run manually check the hashes of the files before writing to the
    # table.
    column_names = ['log_sensor_file_id', 'src_dirname', 'dest_dirname', 'fname',
                    'time_verified', 'rsync_operation', 'is_deleted', 'is_finished']
    db_rows = list()
    for this_out in out:
        if this_out.startswith('>f'):
            try:
                operation, fname, date_copied, time_verified = this_out.split(' ')
            except:
                continue

            df = sensor_file_table.query(
                where=f"sensor_file_path @> ARRAY['{fname}']").reset_index()

            if len(df.log_sensor_file_id) > 0:
                log_sensor_file_id = df.log_sensor_file_id[0]
            else:
                print(f'log_sensor_file_id not found for {fname}')
                continue

            # ensure trailing slash (but don't provide to rsync)
            dest_dir = os.path.join(dest_dir, '')
            src_dir = os.path.join(src_dir, '')
            db_rows.append((log_sensor_file_id, src_dir, dest_dir,
                            fname, f'{date_copied}_{time_verified}', operation,
                            False, False))

    db_table.insert_rows(db_rows, column_names)

    t1 = time.time()
    # XXX: If Python process dies or interrupts the rsync, then we won't have
    # *any* of the rsync transfers from that run written to the log_file table.
    out = subprocess.run(["rsync", src_dir, dest_dir, '-a',
                          "--out-format=%i %n%L %t"],
                         capture_output=True)

    t2 = time.time()
    print(f'Time taken by rsync is {(t2 - t1) / 3600.} hours')
    _update_copystatus(db_table, show_unfinished=False)

    return db_rows

#
#  /a/b/c file.txt
#  /a/b/  c/file.txt

# find files in /a/b/


def delete_files(db_table, target_dir, suitable_dest_dir, threshold=0.9,
                 older_than=30, dry_run=False):
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
    dry_run : int
        If True, will run all the database operations but not actually
        delete the files. Pass a copy of the log_file table to db_table
        for the dry run to avoid inconsistencies.

    Notes
    -----
    Let's say the data was first written to "NAS", then copied to "who"
    and from "who" to "neo":

    .. cssclass:: table-striped

    +-----+-----+------------+
    | src | dest| is_deleted |
    +=====+=====+============+
    | NULL| NAS |   False    |
    +-----+-----+------------+
    | Nas | who |   False    |
    +-----+-----+------------+
    | who | neo |   False    |
    +-----+-----+------------+

    Now, if we want to delete the file from "Nas" and ensure that it has
    been copied to the *final* destination "neo", we provide the following
    arguments to the function:

    target_dir = NAS, suitable_dest = neo

    .. cssclass:: table-striped

    +-----+-----+------------+
    | src | dest| is_deleted |
    +=====+=====+============+
    | NULL| NAS |   True     |
    +-----+-----+------------+
    | NAS | who |   False    |
    +-----+-----+------------+
    | who | neo |   False    |
    +-----+-----+------------+

    Note that ``is_deleted`` bool is set to ``True`` where dest = target_dir

    Here is another example:

    target_dir = who, suitable_dest = neo

    .. cssclass:: table-striped

    +-----+-----+------------+
    | src | dest| is_deleted |
    +=====+=====+============+
    | NULL| NAS |   True     |
    +-----+-----+------------+
    | NAS | who |   True     |
    +-----+-----+------------+
    | who | neo |   False    |
    +-----+-----+------------+
    """
    if dry_run:
        assert 'copy' in db_table.table_id

    # ensure trailing slash
    target_dir = os.path.join(target_dir, '')
    suitable_dest_dir = os.path.join(suitable_dest_dir, '')

    stats = shutil.disk_usage(target_dir)
    fraction_occupied = stats.used / stats.total
    print(f'Fraction {fraction_occupied} is occupied')
    if fraction_occupied < threshold:
        return

    where = f"dest_dirname='{target_dir}' "
    where += f"AND DATE_PART('day', AGE(current_timestamp, time_verified)) > {older_than} "
    where += "AND is_deleted=False AND is_finished=True"
    fnames_to_delete = db_table.query(where=where).fname

    if len(fnames_to_delete) == 0:
        print('No files to delete in dest_dirname that are older than'
              f'{older_than} days')
        return

    fnames = [f"\'{fname}\'" for fname in fnames_to_delete.tolist()]
    fnames = ', '.join(fnames)
    where = f"dest_dirname!='{suitable_dest_dir}' "
    where += f"AND src_dirname IS NOT NULL "  # exclude write operations
    where += "AND is_deleted=False AND is_finished=True"  # just to be safe
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
        if os.path.exists(fname):
            print(f'Deleting ... {fname}')
            if not dry_run:
                os.remove(fname)
        else:
            print(f'File not found while deleting ... {fname}\n')

        # XXX: might be a little inefficient to loop file-by-file but
        # probably more robust in case of failure.

        db_table.insert_rows([(operation_id, True,)],
                             cols=['operation_id', 'is_deleted'],
                             on_conflict='update')
