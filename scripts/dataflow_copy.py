# Authors: Mainak Jas <mjas@harvard.mgh.edu>
#        : Siddharth Patel <spatel@phmi.partners.org>

import psycopg2
import os

from neurobooth_terra import Table, create_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import copy_files

from config import ssh_args, db_args

src_dir = '/autofs/nas/neurobooth/data_test/'
### TODO: Add logic here later when neo/3 and drwho/3 get full
dest_dir_1 = '/space/neo/3/neurobooth/data_test/'
dest_dir_2 = '/space/drwho/3/neurobooth/data_test/'
table_id = 'log_file_copy'

# get all sessions living in NAS
sessions = []
for (_, session_folders, _) in os.walk(src_dir):
    sessions.extend(session_folders)
    break
# remove session 'old' that's a data dump of irrelevant data
if 'old' in sessions:
    sessions.remove('old')

# Copying data to two separate destinations based on odd/even subject_ids
with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        sensor_file_table = Table('log_sensor_file', conn)
        db_table = Table(table_id, conn)
        # copy data session by session
        for session in sessions:
            # set source dir - NAS
            trg_dir = os.path.join(src_dir, session)
            # set dest dir - neo if subject id odd, else drwho
            subj_id = int(session.split('_')[0])
            if subj_id % 2: #odd
                dest_dir = os.path.join(dest_dir_1, session)
            else: #even
                dest_dir = os.path.join(dest_dir_2, session)

            # Note: trg_dir and dest_dir do not have trailing slashes here!
            copy_files(trg_dir, dest_dir, db_table, sensor_file_table)

# For rsync it does not matter if trg_dir has a trailing slash,
# however dest_dir must have a trailing slash during a copy run.
# In this code, we pass dest_dir without a trailing slash, because
# we do a dry run first, and the dry run should be done without
# a trailing slash. Later we add trailing slashes before the
# actual copy run.