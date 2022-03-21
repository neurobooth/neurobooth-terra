"""Ingest subject table to database."""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import psycopg2
import pandas as pd

from neurobooth_terra.redcap import fetch_survey, dataframe_to_tuple
from neurobooth_terra.postgres import Table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder

from config import ssh_args, db_args, project

survey_id = 99915

# TODOs
# cascading should not be deleting.
# what if test subject matches but no old_record_id or old_record_id not matching?
# what happens on conflict, how to update -> then cascade (not filenames for now)

df = fetch_survey(project, survey_name='subject',
                  survey_id=survey_id)
df = df.rename(columns={'record_id': 'subject_id',
                        'old_record_id': 'old_subject_id'})
df = df[~pd.isna(df[f'end_time_subject'])]
rows_subject, cols_subject = dataframe_to_tuple(
    df,
    df_columns=['subject_id', 'redcap_event_name',
                'first_name_birth', 'middle_name_birth',
                'last_name_birth', 'date_of_birth', 'country_of_birth',
                'gender_at_birth', 'birthplace'])

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        table_subject = Table('subject', conn)
        table_subject.insert_rows(rows_subject, cols_subject,
                                  on_conflict='update',
                                  conflict_cols=['subject_id'])
        table_subject.insert_rows(rows_subject, cols_subject,
                                  on_conflict='update',
                                  conflict_cols=['first_name_birth',
                                                 'last_name_birth',
                                                 'date_of_birth'],
                                  update_cols=['subject_id', 'old_subject_id'])
