# Authors: Mainak Jas <mjas@harvard.mgh.edu>
#        : Siddharth Patel <spatel@phmi.partners.org>

import psycopg2
import os
import shutil
import json

from neurobooth_terra import Table, create_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import copy_files

from config import ssh_args, db_args


# A note about setting the value of reserve_threshold in
# the config json:
# 
# The reserve_threshold value must be in units of bytes
# A specific TB value can be expressed as TB*1024**4
# For example a reserve_threshold of 2TB can be expressed
# as 2*1024**4 = 2199023255552
# 
# The value of reserve_threshold should be chosen such that
# it is 3 to 4 times the amount of data generated per session,
# and leaves about 2% of the total disk memory empty
# 
# A note about suitable_volumes in the config json:
# The destination directory paths should exist.


def get_volume_to_fill(volumes: list, threshold: int) -> str:
    ''' Return the most filled volume that is below reserve_threshold
    '''
    vol_disk_usage = {}
    for vol in volumes:
        stats = shutil.disk_usage(vol)
        if stats.free > threshold:
            vol_disk_usage[vol] = stats.used

    if len(vol_disk_usage) <= 1:
        raise ValueError(f'Only {len(vol_disk_usage)} available volume left, aborting copy till more volumes are added')

    return max(vol_disk_usage, key=vol_disk_usage.get)


def check_if_copied(session: str, volumes: list) -> tuple:
    ''' Checks if a session is already copied to one of the volumes,
        and returns the volume path if copied
    '''
    for vol in volumes:
        if os.path.exists(os.path.join(vol, session)):
            return True, os.path.join(vol, session)
    return False, ''


configs = json.load(open('../dataflow_config.json'))
suitable_volumes = configs['suitable_volumes']  # list
reserve_threshold = configs['reserve_threshold']  # int


# ---- Printing disk usage statistics ---- #
print("Disk Usage Statistics\n")
print("Volume\t\t\t\t\tTotal (TB)\tUsed (TB)\tAvailable (TB)")
for vol in suitable_volumes:
    stats = shutil.disk_usage(vol)
    print(vol, round(stats.total/1024**4, 2), round(stats.used/1024**4, 2), round(stats.free/1024**4, 2), sep='\t\t')
print(f'\nreserve_threshold set at {round(reserve_threshold/1024**4, 2)} TB\n')
# ---------------------------------------- #


src_dir = '/autofs/nas/neurobooth/data/'
table_id = 'log_file'
dry_run = True


# get all sessions living in NAS
sessions = []
for (_, session_folders, _) in os.walk(src_dir):
    sessions.extend(session_folders)
    break
# remove session 'old' that's a data dump of irrelevant data
if 'old' in sessions:
    sessions.remove('old')


# Copying data
with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        sensor_file_table = Table('log_sensor_file', conn)
        db_table = Table(table_id, conn)
        # copy data session by session
        for session in sessions:
            # set source dir - NAS
            trg_dir = os.path.join(src_dir, session)
            # check if session is already copied
            is_copied, dest_dir = check_if_copied(session, suitable_volumes)
            if not is_copied:
                dest_dir = os.path.join(get_volume_to_fill(suitable_volumes, reserve_threshold), session)
                print(f'copying new session {session} to {dest_dir}')
            else:
                print(f'copying already copied session {session} to {dest_dir}')

            # if not dry_run:
            #     # Note: trg_dir and dest_dir do not have trailing slashes here!
            #     copy_files(trg_dir, dest_dir, db_table, sensor_file_table)

# For rsync it does not matter if trg_dir has a trailing slash,
# however dest_dir must have a trailing slash during a copy run.
# In this code, we pass dest_dir without a trailing slash, because
# we do a dry run first, and the dry run should be done without
# a trailing slash. Later we add trailing slashes before the
# actual copy run.