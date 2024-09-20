# Authors: Siddharth Patel <spatel@phmi.partners.org>

# script to edit log_file table to add trailing slashes
# to source and destination dir, and add session prefix
# to fnames

import psycopg2

from sshtunnel import SSHTunnelForwarder
from neurobooth_terra import Table
import credential_reader as reader
import os

ssh_args = dict(
        ssh_address_or_host='neurodoor.nmr.mgh.harvard.edu',
        ssh_username='sp1022',
        host_pkey_directories='C:\\Users\\siddh\\.ssh',
        #ssh_pkey='~/.ssh/id_rsa',
        remote_bind_address=('192.168.100.1', 5432),
        local_bind_address=('localhost', 6543),
        allow_agent=False
)

db_args = reader.read_db_secrets()

table_id = 'log_file'

### Adding trailing slashes
# only copy operation src and dest dir have missing trailing slash
where = f"src_dirname is not null"

with SSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:
        
        db_table = Table(table_id, conn)
        df = pd.DataFrame()
        df = db_table.query(where=where)
        
        for operation_id, src_dirname, dest_dirname in zip(df.index,
                                                           df.src_dirname,
                                                           df.dest_dirname
                                                          ):
            # print(operation_id, src_dirname+'/', dest_dirname+'/')
            db_table.insert_rows([(operation_id, src_dirname+'/', dest_dirname+'/')],
                                 ['operation_id', 'src_dirname', 'dest_dirname'], on_conflict='update')

### Adding session prefix
# json and asc files dont have session prefix in log_sensor_file table
def add_session_prefix(fname):
    if fname.endswith('.json') or fname.endswith('.asc'):
        return fname
    subj_id = fname.split('_')[0]
    date = fname.split('_')[1]
    session = subj_id+'_'+date
    return session+'/'+fname
    
where = f"src_dirname is null"

with SSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:
        
        db_table = Table(table_id, conn)
        df = pd.DataFrame()
        df = db_table.query(where=where)
        
        for operation_id, fname in zip(df.index,
                                       df.fname.apply(lambda x: add_session_prefix(x))):
            # print(operation_id, fname)
            db_table.insert_rows([(operation_id, fname)],
                                 ['operation_id', 'fname'], on_conflict='update')
