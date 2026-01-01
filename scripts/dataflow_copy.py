import os
import shutil
import psycopg2

from neurobooth_terra import Table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from neurobooth_terra.dataflow import copy_files

from config import ssh_args, log_db_args, dataflow_configs


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

    if len(vol_disk_usage) <= dataflow_configs['free_volume_threshold']:
        raise ValueError(f'Only {len(vol_disk_usage)} available volume left, aborting copy till more volumes are added')

    return max(vol_disk_usage, key=vol_disk_usage.get)


def check_if_copied(session: str, volumes: list[str]) -> tuple[bool, str]:
    ''' Checks if a session is already copied to one of the volumes,
        and returns the volume path if copied
    '''
    for vol in volumes:
        session_path = os.path.join(vol, session)
        if os.path.exists(session_path):
            return True, session_path
    return False, ''


suitable_volumes: list = dataflow_configs['suitable_volumes']
reserve_threshold: int = dataflow_configs['reserve_threshold_bytes']


# ---- Printing disk usage statistics ---- #
TERRABYTE = 1024**4  # 1TB = 1024 bytes ** 4
print("Disk Usage Statistics\n")
print(f"{'Volume':_>45}{'Total (TB)':_>20}{'Used (TB)':_>20}{'Available (TB)':_>20}")
for vol in suitable_volumes:
    stats = shutil.disk_usage(vol)
    print(f'{str(vol):_>45}{str(round(stats.total/TERRABYTE, 2)):_>20}{str(round(stats.used/TERRABYTE, 2)):_>20}{str(round(stats.free/TERRABYTE, 2)):_>20}')
print(f'\nreserve_threshold set at {reserve_threshold/TERRABYTE:.2f} TB\n')
# ---------------------------------------- #


src_dir = dataflow_configs['NAS']
table_id = 'log_file'
dry_run = False


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
    with psycopg2.connect(**log_db_args) as conn:

        sensor_file_table = Table('log_sensor_file', conn)
        db_table = Table(table_id, conn)
        # copy data session by session
        for session in sessions:
            # set source dir - NAS
            trg_dir = os.path.join(src_dir, session)
            # set to existing dest_dir if session is already copied
            is_copied, dest_dir = check_if_copied(session, suitable_volumes)
            # if copying a new session for the first time, set dest_dir based on remaining space
            if not is_copied:
                dest_dir = os.path.join(get_volume_to_fill(suitable_volumes, reserve_threshold), session)
                print(f'copying new session {session} to {dest_dir}')

            if not dry_run:
                # Note: trg_dir and dest_dir do not have trailing slashes here!
                copy_files(trg_dir, dest_dir, db_table, sensor_file_table)

# For rsync it does not matter if trg_dir has a trailing slash,
# however dest_dir must have a trailing slash during a copy run.
# In this code, we pass dest_dir without a trailing slash, because
# we do a dry run first, and the dry run should be done without
# a trailing slash. Later we add trailing slashes before the
# actual copy run.