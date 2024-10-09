# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import psycopg2

from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
from config import ssh_args, db_args

from neurobooth_terra import Table


def sanitize_date(s):
    return s[0].strftime('%Y-%m-%d')


with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(**db_args) as conn:

        table_task = Table('log_task', conn)
        table_session = Table('log_session', conn)

        task_df = table_task.query(where='log_session_id IS NULL AND date_times IS NOT NULL')
        session_df = table_session.query().reset_index()

        task_df['date_times'] = task_df['date_times'].apply(sanitize_date)
        task_groups = task_df.groupby(by=['subject_id', 'date_times'])

        log_session_id = int(session_df.log_session_id.max() + 1)
        for group, df in task_groups:
            table_session.insert_rows(
                    cols=['log_session_id', 'subject_id', 'date', 'application_id'],
                    vals=[(log_session_id, group[0], group[1], 'neurobooth_os')])
            for ix in df.index:
                table_task.insert_rows(
                                cols=['log_task_id', 'log_session_id'],
                                vals=[(ix, log_session_id,)],
                                on_conflict='update')
            log_session_id += 1
