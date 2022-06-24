# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import shutil
import psycopg2

from neurobooth_terra import Table, copy_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import delete_files

from config import ssh_args, db_args

target_dir = '/autofs/nas/neurobooth/data_test/'
suitable_dest_dir = '/autofs/nas/neurobooth/data_test_backup/'

dry_run = True
table_id = 'log_file'

# why is rsync so slow?
# don't run rsync on weekend
# run this file on weekend.

if dry_run:
    stats = shutil.disk_usage(target_dir)
    threshold = stats.used / stats.total - 0.1  # ensure that it deletes
    older_than = 1
else:
    threshold = 0.9
    older_than = 30

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

        delete_files(db_table, target_dir, suitable_dest_dir,
                     threshold=threshold, older_than=older_than, dry_run=dry_run)
