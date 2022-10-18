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


def write_files(sensor_file_table, db_table, dest_dir_session):
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
    _, session_name = os.path.split(dest_dir_session)
    dest_dir = os.path.join(dest_dir_session, '')  # ensure trailing slash

    log_file_df = db_table.query(where=f"dest_dirname='{dest_dir}'")
    sensor_file_df = sensor_file_table.query()

    # assuming one sensor_file per row

    # This assumption is false since sensors such as intel have
    # duplicate sensor_file_paths but unique sensor_file_ids for
    # depth vs rgb - same holds true for mbients that have one file
    # but two sensor_file_ids for accelerometer and gyroscope

    def concat_list_elements(x):
        c=''
        for i in x:
            c = c+str(i)
        return c
    sensor_file_df['to_detect_duplicates'] =  sensor_file_df['sensor_file_path'].apply(concat_list_elements)

    sensor_fnames_df = sensor_file_df.drop_duplicates(subset='to_detect_duplicates', keep='first').sensor_file_path.tolist()
    sensor_file_ids = sensor_file_df.drop_duplicates(subset='to_detect_duplicates', keep='first').index.tolist()
    sensor_fnames = list()
    for sensor_fname_row, sensor_file_id in zip(sensor_fnames_df, sensor_file_ids):
        for this_sensor_fname in sensor_fname_row:
            if this_sensor_fname.startswith(session_name):
                sensor_fnames.append((sensor_file_id, this_sensor_fname))

    missing_fnames = [(sensor_file_id, fname)
                      for sensor_file_id, fname in sensor_fnames
                      if fname not in log_file_df.fname.tolist()]

    column_names = ['log_sensor_file_id', 'src_dirname', 'fname',
                    'dest_dirname', 'time_verified', 'rsync_operation',
                    'is_deleted']
    for sensor_file_id, fname in missing_fnames:
        
        if os.path.exists(os.path.join(dest_dir, os.path.split(fname)[-1])):
            time_verified = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            db_table.insert_rows([(sensor_file_id, None, fname,
                                 dest_dir, time_verified, None, False)],
                                 cols=column_names)
        else:
            if not any(ext in fname for ext in ['asc', 'xdf', 'txt', 'csv', 'jittered']):
                print(f'{fname} exists in log_sensor_file table, but does not exist in {dest_dir}')


def _do_files_match(src_dirname, dest_dirname, fname):
    """Compare two files using a hash."""

    # we could also generate hash in Python but reading the file in Python
    # may be more memory intense.
    out_src = subprocess.run(["shasum", os.path.join(src_dirname, os.path.split(fname)[-1])],
                             capture_output=True).stdout.decode('ascii')
    out_dest = subprocess.run(["shasum", os.path.join(dest_dirname, os.path.split(fname)[-1])],
                              capture_output=True).stdout.decode('ascii')

    hash_src, hash_dest = out_src.split(' ')[0], out_dest.split(' ')[0]
    if hash_src != hash_dest:  # could be partially copied?
        print(f'hash of file {fname} does not match: '
              f'({hash_src}, {hash_dest})')
        return False
    return True


def _update_copystatus(db_table, show_unfinished=False):
    """Update copy status after checking if files match"""
    log_file_df = db_table.query(where='is_finished=False')
    for operation_id, log_file_row in log_file_df.iterrows():
        ### bag and avi files are large and uneditable, hence their hashes are not checked explicitly
        if log_file_row['fname'].endswith('.bag') or log_file_row['fname'].endswith('.avi') or _do_files_match(log_file_row['src_dirname'],
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
    # Trailing slash should NOT be present on SOURCE directory for dry run
    out = subprocess.run(["rsync", src_dir, dest_dir, '-a', '--dry-run',
                          "--out-format=%i %n%L %t"],
                         capture_output=True)
    if len(out.stderr) > 0:
        warnings.warn(out.stderr.decode('ascii'))

    out = out.stdout.decode('ascii').split('\n')

    # >f tells us that a file will be transferred but it does not tell us
    # if rsync did actually manage to finish the transfer. Therefore, we
    # will manually check the hashes of the files before writing to the
    # table.
    column_names = ['log_sensor_file_id', 'src_dirname', 'dest_dirname', 'fname',
                    'time_verified', 'rsync_operation', 'is_deleted', 'is_finished']
    db_rows = list()
    for this_out in out:
        if this_out.startswith('>f'):
            if len(this_out.split(' ')) == 4:
                operation, fname, date_copied, time_verified = this_out.split(' ')
            else: ## Edge case for if a file name has spaces in it
                operation = this_out.split(' ')[0]
                fname = " ".join(this_out.split(' ')[1:-2])
                date_copied = this_out.split(' ')[-2]
                time_verified = this_out.split(' ')[-1]
            
            df = sensor_file_table.query(
                where=f"sensor_file_path @> ARRAY['{fname}']").reset_index()

            if len(df.log_sensor_file_id) > 0:
                log_sensor_file_id = df.log_sensor_file_id[0]
            else:
                if not any(ext in fname for ext in ['asc', 'xdf', 'txt', 'csv', 'jittered']):
                    print(f'log_sensor_file_id not found for {fname}')
                continue

            time_current_verification = datetime.datetime.strptime(f'{date_copied}_{time_verified}', "%Y/%m/%d_%H:%M:%S")
            qry = db_table.query(where=f"fname = '{fname}' and is_finished is True")
            num_of_days = 1000
            if len(qry)>0:
                time_previously_verified = qry.iloc[-1].time_verified.to_pydatetime()
                td =  time_current_verification - time_previously_verified
                if td.total_seconds() < num_of_days*24*3600:
                    continue

            # Adding trailing slash before adding to database
            dest_dir = os.path.join(dest_dir, '')
            src_dir = os.path.join(src_dir, '')
            db_rows.append((log_sensor_file_id, src_dir, dest_dir,
                            fname, f'{date_copied}_{time_verified}', operation,
                            False, False))

    db_table.insert_rows(db_rows, column_names)

    # Ensuring trailing slash before rsyncing
    dest_dir = os.path.join(dest_dir, '')
    src_dir = os.path.join(src_dir, '')

    t1 = time.time()
    # XXX: If Python process dies or interrupts the rsync, then we won't have
    # *any* of the rsync transfers from that run written to the log_file table.
    out = subprocess.run(["rsync", src_dir, dest_dir, '-a',
                          "--out-format=%i %n%L %t"],
                         capture_output=True)

    t2 = time.time()
    print(f'Time taken by rsync is {datetime.timedelta(seconds=(t2 - t1))} h:m:s')

    t1 = time.time()
    _update_copystatus(db_table, show_unfinished=False)
    t2 = time.time()
    print(f'Time taken for individual hash checks is {datetime.timedelta(seconds=(t2 - t1))} h:m:s')

    return db_rows

#
#  /a/b/c file.txt
#  /a/b/  c/file.txt

# find files in /a/b/


def delete_files(db_table, target_dir, suitable_dest_dir1, suitable_dest_dir2,
                 threshold=0.85, record_older_than=45, copied_older_than=30,
                 dry_run=False):
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
        print('This is a dry run - nothing will be deleted')
    else:
        print('THIS IS NOT A DRY RUN - Please review deleted files')

    # ensure trailing slash
    target_dir = os.path.join(target_dir, '')
    suitable_dest_dir1 = os.path.join(suitable_dest_dir1, '')
    suitable_dest_dir2 = os.path.join(suitable_dest_dir2, '')

    stats = shutil.disk_usage(target_dir)
    fraction_occupied = stats.used / stats.total

    print(f'Deleting files older than: {record_older_than} days in {target_dir} and '
          f'copied successfully more than: {copied_older_than} days ago')
    print(f'Threshold is set at: {round(threshold*100, 2)}%')
    print(f'Fraction: {round(fraction_occupied*100, 2)}% is occupied')

    if fraction_occupied < threshold:
        print('Threshold not reached: nothing to delete')
        return

    ### Query for files in NAS that are older than 45 days and need to be deleted ###
    # These are write operation files, where src_dirname is null and 
    # dest_dirname is NAS
    # Therefore query all files where dest_dirname is NAS and where 
    # time_verified is more than {older_than} days old, 
    # and is_deleted is False
    # The is_finished will always be Null because these files are copied
    # from CTR/ACQ/STM to NAS via ROBOCOPY and we do not check if hashes match
    # (i.e. we treat NAS as source)
    where = f"position('{target_dir}' in dest_dirname)>0 "
    where += f"AND EXTRACT(EPOCH FROM (current_timestamp - time_verified)) > {record_older_than} " # get seconds - for testing
    # where += f"AND DATE_PART('day', AGE(current_timestamp, time_verified)) > {record_older_than} " # get days - for production
    where += "AND is_deleted=False AND is_finished is null"
    fnames_to_delete_df = db_table.query(where=where)
    fnames_to_delete = fnames_to_delete_df.fname

    # # comment out block if testing/debugging
    # if 'EPOCH' in where:
    #     assert dry_run, "Using seconds to measure age of files instead of days - resolve or set dry_run to True"

    ### Query for subset of files from above that got copied to destination 30 days ago ###
    # dest_dirname is either of suitable_dest, src_dirname is not null (i.e. is NAS,
    # but could be an alternate source as well), is_deleted is False (because redundancy),
    # is_finished is True - i.e. copied successfully with has check, and age is older
    # than 30 days
    where = f"(position('{suitable_dest_dir1}' in dest_dirname)>0 OR position('{suitable_dest_dir2}' in dest_dirname)>0) "
    where += f"AND src_dirname IS NOT NULL " # exclude write operations
    where += "AND is_deleted=False AND is_finished=True " # just to be safe
    where += f"AND EXTRACT(EPOCH FROM (current_timestamp - time_verified)) > {copied_older_than}" # get seconds - for testing
    # where += f"AND DATE_PART('day', AGE(current_timestamp, time_verified)) > {copied_older_than}" # get days - for production
    fnames_transferred_df = db_table.query(where=where)


    ### Find the union between files that need to be deleted and files that are successfully transferred ###
    # removing session prefix and retaining only filename in fname column
    fnames_to_delete_df['fname'] = fnames_to_delete_df.fname.apply(lambda x: os.path.split(x)[-1])
    fnames_transferred_df['fname'] = fnames_transferred_df.fname.apply(lambda x: os.path.split(x)[-1])
    # resetting index so that operation_id is part of df as its own column
    # resetting only fnames_to_delte_df because these operation_ids will be updated in table
    # we don't want to update operation_ids in fnames_transferred_df
    fnames_to_delete_df.reset_index(inplace=True)
    # finding union between sets via an inner join
    files_to_delete_union_df = fnames_to_delete_df.merge(fnames_transferred_df,
                                                         how='inner',
                                                         left_on='fname',
                                                         right_on='fname')
    # saving files to delete union df as csv
    outfile = datetime.datetime.now().strftime("%Y-%m-%d_%Hh%Mm%Ss")+'_'+'delete_union_dataframe.csv'
    files_to_delete_union_df.to_csv(outfile)


    ### Deleting files ###
    for ix, files_row in files_to_delete_union_df.iterrows():

        operation_id = files_row.operation_id
        fname = os.path.join(files_row.dest_dirname_x, files_row.fname)

        if os.path.exists(fname):
            ### Check by querying row by operation_id
            where = f'operation_id={operation_id}'
            assert_df = db_table.query(where=where)
            if (
                len(assert_df)==1
                and target_dir in assert_df.dest_dirname.iloc[0]
                and assert_df.is_deleted.iloc[0]==False
                and assert_df.is_finished.iloc[0]==None
                ):
                print(f'Deleting ... {fname}')
                if not dry_run:
                    os.remove(fname)
                    db_table.insert_rows([(operation_id, True,)],
                                        cols=['operation_id', 'is_deleted'],
                                        on_conflict='update')
            else:
                print(f'Query on operation_id {operation_id} failed before delete: {assert_df}')
        else:
            print(f'File not found while deleting ... {fname}')

        # XXX: might be a little inefficient to loop file-by-file but
        # probably more robust in case of failure.

