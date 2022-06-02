# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import psycopg2

from neurobooth_terra import Table, create_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import write_files

from config import ssh_args, db_args

do_create_table = False
write_table = True
dest_dir = '/autofs/nas/neurobooth/data_test/'

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        if do_create_table:
            table_id = 'log_file'
            column_names = ['operation_id', 'log_sensor_file_id', 'src_dirname',
                            'dest_dirname', 'fname', 'time_verified',
                            'rsync_operation', 'is_deleted']
            dtypes = ['SERIAL', 'text', 'text',
                      'text', 'text', 'timestamp', 'text',
                      'boolean']
            create_table(table_id, conn, column_names, dtypes,
                         primary_key='operation_id',
                         foreign_key={'log_sensor_file_id': 'log_sensor_file'})

        if write_table:
            sensor_file_table = Table('log_sensor_file', conn)
            db_table = Table('log_file', conn)
            write_files(sensor_file_table, db_table, dest_dir)
