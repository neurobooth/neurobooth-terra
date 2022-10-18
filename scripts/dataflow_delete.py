# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import shutil
import psycopg2

from neurobooth_terra import Table, copy_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import delete_files

from config import ssh_args, db_args

target_dir = '/autofs/nas/neurobooth/data_test/' # The directory from where files will be deleted

### TODO: Add logic here later when neo/3 and drwho/3 get full
suitable_dest_dir1 = '/space/neo/3/neurobooth/data_test/'
suitable_dest_dir2 = '/space/drwho/3/neurobooth/data_test/'

dry_run = False
table_id = 'log_file_copy' # log_file if target_dir is 'neurobooth/data'

if dry_run:
    stats = shutil.disk_usage(target_dir)
    threshold = stats.used / stats.total - 0.1  # ensure that it deletes
    record_older_than = 20 # days
    copied_older_than = 15 # days
else:
    threshold = 0.85
    record_older_than = 45 # days
    copied_older_than = 30 # days

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        # if dry_run:
        #     copy_table(src_table_id=table_id,
        #                target_table_id=table_id + '_copy',
        #                conn=conn)
        #     db_table = Table(table_id + '_copy', conn)
        # else:
        #     db_table = Table(table_id, conn)

        db_table = Table(table_id, conn)

        delete_files(db_table,
                     target_dir,
                     suitable_dest_dir1,
                     suitable_dest_dir2,
                     threshold=threshold,
                     record_older_than=record_older_than,
                     copied_older_than=copied_older_than,
                     dry_run=dry_run)
