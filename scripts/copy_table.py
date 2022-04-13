"""Script that creates new table log_session and links it with log_task."""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import psycopg2

from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from config import ssh_args, db_args

from neurobooth_terra import Table

update_log_session = False
update_log_task = False
with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        table_task = Table('log_task', conn)
        table_session = Table('log_session', conn)
        task_df = table_task.query().reset_index()

        cols = ['subject_id', 'date', 'study_id',
                'collection_id', 'staff_id', 'application_id']
        rows = list()
        log_task_ids = list()
        for _, row in task_df.iterrows():
            date_time = None
            if row.date_times is not None:
                date_time = row.date_times[0].strftime('%Y-%m-%d')
            rows.append((row.subject_id, date_time, 'study1',
                         row.collection_id, 'AN', 'neurobooth_os'))
            log_task_ids.append(row.log_task_id)

        if update_log_session:
            table_session.insert_rows(rows, cols)

        # now let us insert the foreign key to log_session
        session_df = table_session.query().reset_index()
        rows = [(log_task_ids[row_idx], row.log_session_id)
                for row_idx, (_, row) in enumerate(session_df.iterrows())]
        columns = ['log_task_id', 'log_session_id']

        if update_log_task:
            table_task.insert_rows(rows, columns, on_conflict='update',
                                   update_cols=['log_session_id'])
