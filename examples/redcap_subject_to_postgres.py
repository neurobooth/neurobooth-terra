"""Ingest subject table to database."""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import psycopg2
import pandas as pd

from neurobooth_terra.redcap import fetch_survey, dataframe_to_tuple, rename_subjects
from neurobooth_terra.postgres import Table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder

from config import ssh_args, db_args, project

survey_id = 99915

# TODOs
# cascading should not be deleting.
# what if test subject matches but no old_record_id or old_record_id not matching?
# cascading of filenames

redcap_df = fetch_survey(project, survey_name='subject',
                         survey_id=survey_id, cast_dtype=False)
redcap_df = redcap_df.rename(columns={'record_id': 'subject_id',
                                      'old_record_id': 'old_subject_id',
                                      # XXX: what is the new convention?
                                      'country_of_birth': 'country_of_birth_subject',
                                      'date_of_birth': 'date_of_birth_subject',
                                      'birthplace': 'birthplace_subject',
                                      'gender_at_birth': 'gender_at_birth_subject'})

redcap_df = redcap_df[~pd.isna(redcap_df[f'end_time_subject'])]
rows_subject, cols_subject = dataframe_to_tuple(
    redcap_df,
    df_columns=['subject_id', 'redcap_event_name',
                'first_name_birth', 'middle_name_birth',
                'last_name_birth', 'date_of_birth_subject',
                'country_of_birth_subject', 'gender_at_birth_subject',
                'birthplace_subject'])

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        table_subject = Table('subject', conn)
        rename_subjects(table_subject, redcap_df)

        # intended to handle a change in individual's data
        # 1. If a subject_id exists, update the other columns
        #    (for the subject whose subject_id changed, updating will
        #     have no effect)
        # 2. If it does not exist, then insert it as a new row
        table_subject.insert_rows(rows_subject, cols_subject,
                                  on_conflict='update',
                                  conflict_cols=['subject_id'])
