# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import psycopg2

from neurobooth_terra import Table, create_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import copy_files

from config import ssh_args, db_args

src_dir = '/autofs/nas/neurobooth/data_test/'
dest_dir = '/autofs/nas/neurobooth/data_test_backup/'

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        sensor_file_table = Table('log_sensor_file', conn)
        db_table = Table('log_file', conn)
        copy_files(src_dir, dest_dir, db_table, sensor_file_table)
