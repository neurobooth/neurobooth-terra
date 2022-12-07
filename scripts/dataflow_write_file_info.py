# Authors: Mainak Jas <mjas@harvard.mgh.edu>
#        : Siddharth Patel <spatel@phmi.partners.org>

import psycopg2
import os

from neurobooth_terra import Table, create_table, drop_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import write_files

from config import ssh_args, db_args

do_create_table = False
write_table = True
dest_dir = '/autofs/nas/neurobooth/data_test/'
table_id = 'log_file_copy'

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
            sensor_file_table = Table('log_sensor_file', conn)
            db_table = Table(table_id, conn)
            # write new files in NAS to db, session by session
            for session in sessions:
                dest_dir_session = os.path.join(dest_dir, session)
                write_files(sensor_file_table, db_table, dest_dir_session)

# For testing, set table_id to 'log_file_copy', do_create_table to True and
# write_table to False. Run and check that an empty log_file_copy table
# has been generated.
# Next, set do_create_table to False and write_table to True, and check if
# code runs as expected.