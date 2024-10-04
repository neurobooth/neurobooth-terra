"""
Edit the log_sensor_file table to preprend missing session folder prefixes to file paths.
"""

import psycopg2
import re
import credential_reader as reader

from sshtunnel import SSHTunnelForwarder
from neurobooth_terra import Table

ssh_args = dict(
        ssh_address_or_host='neurodoor.nmr.mgh.harvard.edu',
        ssh_username='bro7',
        # host_pkey_directories='C:\\cygwin64\\home\\bro7\\.ssh',
        ssh_pkey='C:\\cygwin64\\home\\bro7\\.ssh\\id_ed25519',
        remote_bind_address=('192.168.100.1', 5432),
        local_bind_address=('localhost', 6543),
        allow_agent=False
)

db_args = reader.read_db_secrets()

table_id = 'log_sensor_file'
where = f"file_start_time >= '2024-07-01' and file_end_time < '2024-07-03'"


NAME_PATTERN = re.compile(r'(\d+_[\d-]+)_.*')


def add_session_prefix(fname):
    if '/' in fname:
        return fname

    matches = re.match(NAME_PATTERN, fname)
    if matches is None:
        return fname

    session = matches[1]
    return f'{session}/{fname}'


def add_prefix_to_all(files):
    files = list(map(add_session_prefix, files))
    return '{' + ','.join(files) + '}'  # Convert list into postgres array string


with SSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(**db_args) as conn:
        db_table = Table(table_id, conn)
        df = db_table.query(where=where)

        df['sensor_file_path'] = df['sensor_file_path'].apply(add_prefix_to_all)

        for pk, sensor_file_path in zip(df.index, df['sensor_file_path']):
            db_table.update_row(pk, (sensor_file_path,), ['sensor_file_path'])
