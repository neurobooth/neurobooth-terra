"""Script that creates new table log_session and links it with log_task."""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import psycopg2

from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from config import ssh_args, db_args

from neurobooth_terra import Table

# need data governance
# access levels to not change database ad-hoc
# calendar to limit development times
# renaming columns

def sanitize_date(s):
    if s is not None:
        return s[0].strftime('%Y-%m-%d')


import pandas as pd

update_log_session = True
update_log_task = True
with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        table_task = Table('log_task', conn)
        table_session = Table('log_session', conn)
        task_df = table_task.query().reset_index()
        session_df = table_session.query().reset_index()

        task_df['date_times'] = task_df['date_times'].apply(sanitize_date)
        session_df = session_df.astype({'date': 'str'})
        task_df = task_df.rename(columns={'date_times': 'date'})

        task_df_full = pd.merge(session_df, task_df, how='left',
                                on=['subject_id', 'date'])

        # now let us insert the foreign key to log_session
        rows = [(row.log_task_id, row.log_session_id_x)
                for row_idx, (_, row) in enumerate(task_df_full.iterrows())]
        columns = ['log_task_id', 'log_session_id']

        if update_log_task:
            table_task.insert_rows(rows, columns, on_conflict='update',
                                   update_cols=['log_session_id'])
