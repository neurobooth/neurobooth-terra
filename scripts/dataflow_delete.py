# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import psycopg2

from neurobooth_terra import Table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import delete_files

from config import ssh_args, db_args

target_dir = '/autofs/nas/neurobooth/data_test/'
suitable_dest_dir = '/autofs/nas/neurobooth/data_test_backup/'

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        db_table = Table('log_file', conn)
        delete_files(db_table, target_dir, suitable_dest_dir, threshold=0.9,
                     older_than=30)
