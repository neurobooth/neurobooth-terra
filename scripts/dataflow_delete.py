# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import psycopg2

from neurobooth_terra import Table, copy_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import delete_files

from config import ssh_args, db_args

target_dir = '/autofs/nas/neurobooth/data_test/'
suitable_dest_dir = '/autofs/nas/neurobooth/data_test_backup/'

dry_run = True
table_id = 'log_file'

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        if dry_run:
            copy_table(src_table_id=table_id,
                       target_table_id=table_id + '_copy',
                       conn=conn)
            db_table = Table(table_id + '_copy', conn)
        else:
            db_table = Table(table_id, conn)

        delete_files(db_table, target_dir, suitable_dest_dir, threshold=0.9,
                     older_than=30, dry_run=dry_run)
