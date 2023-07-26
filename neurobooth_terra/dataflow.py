"""Utility functions for file transfers."""

# Authors: Mainak Jas <mainakjas@gmail.com>
#        : Siddharth Patel <spatel@phmi.partners.org>

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

    Discovers new files as they appear in NAS and writes then to
    log_file table

    Because we treat NAS as primary source of data, there is no source
    directory, hence 'src_dirname' is set as null, and NAS is treated
    as destination directory, 'dest_dirname'

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

    # get log_file table where dest_dirname is NAS
    log_file_df = db_table.query(where=f"dest_dirname='{dest_dir}'")
    # get log_sensor_file table
    sensor_file_df = sensor_file_table.query()

    # Sensors such as intel have duplicate sensor_file_paths but unique
    # sensor_file_ids for depth vs rgb - same holds true for mbients that
    # have one data file but two sensor_file_ids for accelerometer and
    # gyroscope. Thus we can have duplicate sensor files over two rows

    ### Removing duplicate sensor files, to write one file only once in log_file table
    def concat_list_elements(x):
        c=''
        for i in x:
            c = c+str(i)
        return c
    # Convert an array of filenames into a single concatenated string. If order of filenames in
    # array is not maintained, this logic for detecting duplicates will break. However neurobooth_os
    # generally writes filenames in array to neurobooth_terra in the same order.
    sensor_file_df['to_detect_duplicates'] =  sensor_file_df['sensor_file_path'].apply(concat_list_elements)

    # this is a list of lists - since sensor_file_path is an array in log_sensor_file table
    sensor_fnames_list = sensor_file_df.drop_duplicates(subset='to_detect_duplicates',
                                                        keep='first').sensor_file_path.tolist()
    sensor_file_ids = sensor_file_df.drop_duplicates(subset='to_detect_duplicates',
                                                     keep='first').index.tolist()
    ### end remove duplicates

    # get all sensor file names from log_sensor_file table
    sensor_fnames = list()
    for sensor_fname_row, sensor_file_id in zip(sensor_fnames_list, sensor_file_ids):
        for this_sensor_fname in sensor_fname_row:
            if this_sensor_fname.startswith(session_name):
                sensor_fnames.append((sensor_file_id, this_sensor_fname))

    # TODO: Add code here to get notes.txt, outcomes.csv and results.csv files
    #       from log_task table. Append to sensor_fnames. Replace sensor_file_id
    #       with log_task_id (??)

    # filter files that are new
    missing_fnames = [(sensor_file_id, fname)
                      for sensor_file_id, fname in sensor_fnames
                      if fname not in log_file_df.fname.tolist()]

    # insert into database table
    column_names = ['log_sensor_file_id', 'src_dirname', 'fname',
                    'dest_dirname', 'time_verified', 'rsync_operation',
                    'is_deleted']
    # Above column names correspond to following column values
    # column_values = [(sensor_file_id, None, fname, 
    #                   dest_dir, time_verified, None,
    #                   False)]
    for sensor_file_id, fname in missing_fnames:
        # removing session prefix from sensor file name before building full path-to-file
        if os.path.exists(os.path.join(dest_dir, os.path.split(fname)[-1])):
            time_verified = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            column_values = [(sensor_file_id, None, fname, 
                            dest_dir, time_verified, None,
                            False)]
            db_table.insert_rows(column_values, cols=column_names)
        else:
            # files with these extensions are not tracked yet
            if not any(ext in fname for ext in ['xdf', 'txt', 'csv', 'jittered']):
                print(f'{fname} exists in log_sensor_file table, but does not exist in {dest_dir}')


def _do_files_match(src_dirname, dest_dirname, fname):
    """Compare two files using a hash."""

    # bag and avi files are large and uneditable, hence their hashes are not
    # checked explicitly - instead if the file size and last modified date is
    # the same, function returns true
    if fname.endswith('.bag') or fname.endswith('.avi'):

        src_mtime = os.path.getmtime(os.path.join(src_dirname, os.path.split(fname)[-1]))
        src_fsize = os.path.getsize(os.path.join(src_dirname, os.path.split(fname)[-1]))

        dest_mtime = os.path.getmtime(os.path.join(dest_dirname, os.path.split(fname)[-1]))
        dest_fsize = os.path.getsize(os.path.join(dest_dirname, os.path.split(fname)[-1]))

        if (src_fsize==dest_fsize) and (src_mtime==dest_mtime):
            return True
        else:
            print(f'file size/last modified time of file {fname} did not '
                  f'match between {src_dirname} and {dest_dirname}')
            return False

    # For all other files generate hashes and compare
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
        if _do_files_match(log_file_row['src_dirname'],
                           log_file_row['dest_dirname'],
                           log_file_row['fname']
                           ):
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # strf: '2022-10-18 15:58:38'
            db_table.insert_rows([(operation_id, current_time, True)],
                                 ['operation_id', 'time_verified', 'is_finished'],
                                 on_conflict='update')
        elif show_unfinished:
            db_table.delete_row(where=f"operation_id={operation_id}")
            print(f"The file transfer from {log_file_row['src_dirname']} "
                  f"to {log_file_row['dest_dirname']} did not finish for "
                  f"file {log_file_row['fname']}")


def copy_files(src_dir, dest_dir, db_table, sensor_file_table):
    """Copy files per session using rsync.

    First, an rsync dry run is executed to get details of copy.
    The dry run output is used to track copy operations in the
    log_file table.

    Note: copy_files has been intended to run only after write_files
    has been executed via the write_file_info script. If copy files
    is run without running write_files first, it will still copy
    and add the records to the database table. write_file_script can
    mostly be excuted posthoc as well, and the database state will
    remain fine. Running delete_files will possibly break the database
    state though. (Exhaustive testing of executing write->copy->delete
    out of order has not been done)
    
    Parameters
    ----------
    src_dir : str
        The source directory.
    dest_dir : str
        The destination directory.
    db_table : instance of Table
        The table containing information about the file transfers.
    sensor_file_table : instance of Table
        The table containing information about the sensors used in a session
        and the files.
    """

    # update copy status first, in case process failed on previous run
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
            # parse rsync dry run output
            if len(this_out.split(' ')) == 4:
                operation, fname, date_copied, time_verified = this_out.split(' ')
            else: ## Edge case for if a file name has spaces in it
                operation = this_out.split(' ')[0]
                fname = " ".join(this_out.split(' ')[1:-2])
                date_copied = this_out.split(' ')[-2]
                time_verified = this_out.split(' ')[-1]
            
            # query log_sensor_file table for this specific file
            df = sensor_file_table.query(
                where=f"sensor_file_path @> ARRAY['{fname}']").reset_index()

            # TODO: If query returns empty, check the log_task table for txt/csv file

            if len(df.log_sensor_file_id) > 0:
                log_sensor_file_id = df.log_sensor_file_id[0]
            else:
                # files with these extensions are not tracked yet
                untracked_extensions = ['xdf', 'txt', 'csv', 'jittered', 'asc', 'log']
                if not any(ext in fname for ext in untracked_extensions):
                    print(f'log_sensor_file_id not found for {fname}')
                continue

            # Block to check if file has been edited!
            # If file has already been copied over, check if files match. If they do match - continue
            # Else: the file has been edited - therefore, set is_finished to false and continue.
            # We continue either way because we don't want to rewrite this file to table. The rsync for
            # this session will proceed anyway outside this loop, and edited file will be copied over.
            # Then update copy status will run, and update is_finished to true, and time_verified
            # to when it checks hashes

            # Check if file has already been copied over
            qry = db_table.query(where=f"fname = '{fname}' and is_finished is True")
            qry.reset_index(inplace=True)
            if len(qry)==1:
                # if it has, then check if files match
                if _do_files_match(src_dir, dest_dir, fname):
                    # if files match -> continue
                    continue
                else:
                    # update is_finished to False for the copied but edited file
                    db_table.insert_rows([(int(qry.operation_id[0]), False)],
                                         ['operation_id', 'is_finished'],
                                         on_conflict='update')
                    # and then continue to prevent writing to db table
                    continue

            # Adding trailing slash before adding to database
            dest_dir = os.path.join(dest_dir, '')
            src_dir = os.path.join(src_dir, '')
            # time verified is null, since file hasn't been copied over yet, nor copy status verified yet
            db_rows.append((log_sensor_file_id, src_dir, dest_dir,
                            fname, None, operation,
                            False, False))

    # insert files to table
    db_table.insert_rows(db_rows, column_names)

    # Ensuring trailing slash before rsyncing
    dest_dir = os.path.join(dest_dir, '')
    src_dir = os.path.join(src_dir, '')

    t1 = time.time()
    # XXX: If Python process dies or interrupts the rsync, then the rsync
    # transfer will be recorded in the db table with time_verified as null
    # and is_finished as False. In the next run, update_copystatus will
    # remove these lines from the db table as show_unfinished flag will be
    # set to true
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


def delete_files(db_table, target_dir, suitable_dest_dirs,
                 threshold=0.85, record_older_than=45, copied_older_than=30,
                 dry_run=True):
    """Delete files if x% of disk is filled.

        Delete files from NAS (source directory) when x% of NAS is filled.
        This function has been written to specifically delete files from
        NAS. As such the target_dir should always be NAS.
        The function also needs a list of suitable_dest_directories. New
        destination directories can be added as more volumes are added
        to storage.
        
        Changes needed to make this a generalized function are commented in
        code. To generalize, there should only be one suitable destination
        directory, and this function should be called once for each destination

    Parameters
    ----------
    db_table : instance of Table
        The database table containing information about file transfers. The
        row corresponding to the transferred file is updated with
        is_deleted=True.
    target_dir : str
        The source directory path from which to delete files.
    suitable_dest_dirs : list
        A list of all destination directory paths where the files have been
        copied.
    threshold : float
        A fraction between 0. and 1. If more than threshold fraction of the
        source directory is filled, these files will be deleted.
    record_older_than : int
    copied_older_than : int
        If a file is older than record_older_than days in src_dir and
        copied_older_than in any of suitable_dest_dirs, they will be
        deleted.
    dry_run : boolean
        If True, will run all the database operations but not actually
        delete the files.

    Notes
    -----
    Let's say the data was written to "NAS", then copied to "who"

    .. cssclass:: table-striped

    +-----+-----+------------+
    | src | dest| is_deleted |
    +=====+=====+============+
    | NULL| NAS |   False    |
    +-----+-----+------------+
    | NAS | who |   False    |
    +-----+-----+------------+

    Now, if we want to delete the file from "Nas" and ensure that it has
    been copied to the *final* destination "who", we provide the following
    arguments to the function:

    target_dir = NAS, suitable_dest_dirs = [who, neo, ...]

    .. cssclass:: table-striped

    +-----+-----+------------+
    | src | dest| is_deleted |
    +=====+=====+============+
    | NULL| NAS |   True     |
    +-----+-----+------------+
    | NAS | who |   False    |
    +-----+-----+------------+

    Note that ``is_deleted`` bool is set to ``True`` where dest = target_dir

    """

    if dry_run:
        print('This is a dry run - nothing will be deleted')
    else:
        print('THIS IS NOT A DRY RUN - Please review deleted files')

    # ensure trailing slash
    target_dir = os.path.join(target_dir, '')
    volumes = [os.path.join(vol, '') for vol in suitable_dest_dirs]

    stats = shutil.disk_usage(target_dir)
    fraction_occupied = stats.used / stats.total

    print(f'Deleting files older than: {int(record_older_than/(24*3600))} days in {target_dir} and '
          f'copied successfully more than: {int(copied_older_than/(24*3600))} days ago')
    print(f'Threshold is set at: {(threshold*100):.2f}%')
    print(f'Fraction: {(fraction_occupied*100):.2f}% is occupied')

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
    # (i.e. we treat these files and NAS as source)
    where = f"position('{target_dir}' in dest_dirname)>0 "
    where += f"AND EXTRACT(EPOCH FROM (current_timestamp - time_verified)) > {record_older_than} "
    # is_finished will be True if source is different than NAS - change query to generalize
    where += "AND is_deleted=False AND is_finished is null"
    fnames_to_delete_df = db_table.query(where=where)

    ### Query for subset of files from above that got copied to destination 30 days ago ###
    # dest_dirname is either of suitable_dest, src_dirname is not null (i.e. is NAS,
    # but could be an alternate source as well), is_deleted is False (because redundancy),
    # is_finished is True - i.e. copied successfully with hash check, and age is older
    # than 30 days
    volume_qry_conditions = [f"position('{vol}' in dest_dirname) > 0" for vol in volumes]
    where = '(' + ' OR '.join(volume_qry_conditions) + ') '
    # src_dirname should be {target_dir} - change query to generalize
    where += f"AND src_dirname IS NOT NULL " # exclude write operations
    where += "AND is_deleted=False AND is_finished=True " # just to be safe
    where += f"AND EXTRACT(EPOCH FROM (current_timestamp - time_verified)) > {copied_older_than}"
    fnames_transferred_df = db_table.query(where=where)

    ### Find the union between files that need to be deleted and files that are successfully transferred ###
    # removing session prefix and retaining only filename in fname column
    fnames_to_delete_df['fname'] = fnames_to_delete_df.fname.apply(lambda x: os.path.split(x)[-1])
    fnames_transferred_df['fname'] = fnames_transferred_df.fname.apply(lambda x: os.path.split(x)[-1])

    # resetting index on fnames_to_delte_df to make index:operation_id
    # as its own separate column
    fnames_to_delete_df.reset_index(inplace=True)

    # Removing all columns that are not needed for delete operation
    delete_cols_to_keep = ['operation_id', 'fname', 'dest_dirname']
    fnames_to_delete_df = fnames_to_delete_df[delete_cols_to_keep]
    transferred_cols_to_keep = ['fname'] # Only need 'fname' from transferred_df
    fnames_transferred_df = fnames_transferred_df[transferred_cols_to_keep]

    # finding union between sets via an inner join
    files_to_delete_union_df = fnames_to_delete_df.merge(fnames_transferred_df,
                                                         how='inner',
                                                         left_on='fname',
                                                         right_on='fname')

    ### Deleting files ###
    for ix, files_row in files_to_delete_union_df.iterrows():

        operation_id = files_row.operation_id
        fname = os.path.join(files_row.dest_dirname, files_row.fname)

        if os.path.exists(fname):
            ### Check by querying row by operation_id
            where = f'operation_id={operation_id}'
            assert_df = db_table.query(where=where)
            if (
                # confirm that we only ever get one row,
                # should always be true since operation_id is a primary key
                len(assert_df)==1
                # confirm that fname from db matches fname that will be deleted
                and os.path.split(fname)[-1] == os.path.split(assert_df.fname.iloc[0])[-1]
                # confirm that target dir is in dest_dirname of db record
                and assert_df.dest_dirname.iloc[0].startswith(target_dir)
                # confirm that is_deleted is False for the file, since file is yet to be deleted
                and assert_df.is_deleted.iloc[0]==False
                # confirm that is_finished is None, since we are deleting from NAS
                # change this condition to generalize - this should be True for non NAS
                and assert_df.is_finished.iloc[0]==None
                ):
                print(f'Deleting ... {fname}')
                if not dry_run:
                    os.remove(fname)
                    db_table.insert_rows([(operation_id, True,)],
                                        cols=['operation_id', 'is_deleted'],
                                        on_conflict='update')
            else:
                print(f'Query or checks on operation_id {operation_id} failed before delete: {assert_df}')
        # This else condition triggers in case files were moved to an alternate location/deleted
        else:
            print(f'File not found while deleting ... {fname}')

