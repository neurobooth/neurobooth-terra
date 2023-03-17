# Authors: Mainak Jas <mjas@harvard.mgh.edu>
#        : Siddharth Patel <spatel@phmi.partners.org>

import shutil
import psycopg2

from neurobooth_terra import Table, copy_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import delete_files

from config import ssh_args, db_args

num_secs_in_a_day = 24*3600 # total number of seconds in a day - conversion factor

target_dir = '/autofs/nas/neurobooth/data/' # The directory from where files will be deleted

suitable_dest_dir1 = '/space/neo/3/neurobooth/data/'
suitable_dest_dir2 = '/space/drwho/3/neurobooth/data/'

dry_run = False
table_id = 'log_file' # log_file if target_dir is 'neurobooth/data', log_file_copy for testing

if dry_run:
    stats = shutil.disk_usage(target_dir)
    threshold = stats.used / stats.total - 0.1  # ensure that it deletes
    record_older_than_days = 60 # days
    copied_older_than_days = 45 # days
    # time elapsed is needed in seconds for sql query
    record_older_than = record_older_than_days * num_secs_in_a_day # seconds
    copied_older_than = copied_older_than_days * num_secs_in_a_day # seconds

else:
    threshold = 0.95
    record_older_than_days = 60 # days (divide by num_secs_in_a_day to convert days to seconds for testing)
    copied_older_than_days = 45 # days (divide by num_secs_in_a_day to convert days to seconds for testing)
    # time elapsed is needed in seconds for sql query
    record_older_than = record_older_than_days * num_secs_in_a_day # seconds
    copied_older_than = copied_older_than_days * num_secs_in_a_day # seconds

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        db_table = Table(table_id, conn)

        delete_files(db_table,
                     target_dir,
                     suitable_dest_dir1,
                     suitable_dest_dir2,
                     threshold=threshold,
                     record_older_than=record_older_than,
                     copied_older_than=copied_older_than,
                     dry_run=dry_run)
