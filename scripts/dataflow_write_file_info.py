# Authors: Mainak Jas <mjas@harvard.mgh.edu>
#        : Siddharth Patel <spatel@phmi.partners.org>

import psycopg2
import os

from neurobooth_terra import Table, create_table, drop_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import write_files

from config import ssh_args, db_args


# get deduplicated log_sensor_file table from database
def _dedup_log_sensor_file(sensor_file_df):
    '''returns the deduplicated log_sensor_file table
    
        context:
        Sensors such as intel have duplicate sensor_file_paths but unique
        sensor_file_ids for depth vs rgb - same holds true for mbients that
        have one data file but two sensor_file_ids for accelerometer and
        gyroscope. Thus we can have duplicate sensor files over two rows.
        We want to write one line in the log_file table per device hence
        this function.
    '''
    def __concat_list_elements(x):
        c=''
        for i in x:
            c = c+str(i)
        return c
    # Convert an array of filenames into a single concatenated string. If order of filename array
    # is not maintained, this logic for detecting duplicates will break. However neurobooth_os
    # conserves the filename array between two sensors of same device (eg: acc/gyr) when writing
    # to log_sensor_file table.
    sensor_file_df['to_detect_duplicates'] =  sensor_file_df['sensor_file_path'].apply(__concat_list_elements)
    return sensor_file_df.drop_duplicates(subset='to_detect_duplicates', keep='first')


do_create_table = False
write_table = True
dest_dir = '/autofs/nas/neurobooth/data/'
table_id = 'log_file'

# get all sessions living in NAS
sessions = []
for (_, session_folders, _) in os.walk(dest_dir):
    sessions.extend(session_folders)
    break
# remove session 'old' that's a data dump of irrelevant data
if 'old' in sessions:
    sessions.remove('old')

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        if do_create_table:
            # drop old log_file_copy table
            if 'copy' in table_id:
                drop_table(table_id, conn)
            
            # create a new log_file_copy table
            column_names = ['operation_id', 'log_sensor_file_id', 'src_dirname',
                            'dest_dirname', 'fname', 'time_verified',
                            'rsync_operation', 'is_deleted', 'is_finished']
            dtypes = ['SERIAL', 'text', 'text',
                      'text', 'text', 'timestamp', 'text',
                      'boolean', 'boolean']
            create_table(table_id, conn, column_names, dtypes,
                         primary_key='operation_id',
                         foreign_key={'log_sensor_file_id': 'log_sensor_file'})

        if write_table:
            # get log_sensor_file table and remove duplicates
            sensor_file_table = Table('log_sensor_file', conn)
            sensor_file_df = sensor_file_table.query()
            dedup_log_sensor_file_df = _dedup_log_sensor_file(sensor_file_df)

            db_table = Table(table_id, conn)
            # write new files in NAS to db, session by session
            for session in sessions:
                dest_dir_session = os.path.join(dest_dir, session)
                write_files(dedup_log_sensor_file_df, db_table, dest_dir_session)

# For testing, set table_id to 'log_file_copy', do_create_table to True and
# write_table to False. Run and check that an empty log_file_copy table
# has been generated.
# Next, set do_create_table to False and write_table to True, and check if
# code runs as expected.