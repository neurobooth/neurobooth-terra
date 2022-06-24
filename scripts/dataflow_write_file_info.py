# Authors: Mainak Jas <mjas@harvard.mgh.edu>

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

sessions = []
for (_, session_folders, _) in os.walk(dest_dir):
    sessions.extend(session_folders)
    break

sessions = sessions[2:]

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        if do_create_table:
            
            if 'copy' in table_id:
                drop_table(table_id, conn)
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
            for session in sessions:
                dest_dir_session = os.path.join(dest_dir, session)
                write_files(sensor_file_table, db_table, dest_dir_session)
