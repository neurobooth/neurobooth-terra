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

# xdf and csv are not represented, other files did not get written due to
# non-graceful exists.
# Possible ways to handling retrospective entries added to log_sensor_file:
# 1. Do not affect write_files assuming files still exist on NAS.
#    It will simply copy over the new entries to log_file.
# 2. However, retrospective entries should be added for copy operations.
#    For example, if the data was copied from NAS to DrWho, the subsequent
#    rsync operation will not produce an output for that file and hence
#    it will not be added automatically to log_file.
# 3. For deletion, there are two options:
#    a. Retrospectively adding the data_verified by looking at file header (?)
#    b. Use date_verified as current date and let the deletion happen x days
#       from when the retrospective correction was done.
#
# class Foo:
#    __enter__()
#        pass
#    __exit__():
#        pass
#
# with Foo as foo:
#    # do whatever
