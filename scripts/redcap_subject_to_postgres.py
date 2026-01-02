"""Ingest subject table to database.
"""
import argparse
import psycopg2
import pandas as pd

from neurobooth_terra.redcap import (fetch_survey, dataframe_to_tuple,
                                     rename_subject_ids)
from neurobooth_terra.postgres import Table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder

from config import ssh_args, rc_db_args

############################################
### --- Subject table survey numbers --- ###
#         FA REDCap project survey_id = 197331
# Neurobooth REDCap project survey_id = 99915
############################################

def import_project(project_name: str):
    """ Returns a REDCap project from config
    """
    module = __import__('config')
    return getattr(module, project_name)


def fetch_subject_table(project_name: str, survey_id: int):
    """ Fetches subject table from REDCap project and
        return tuple of rows and columns, and table dataframe
    """
    redcap_project = import_project(project_name)

    redcap_df = fetch_survey(redcap_project, survey_name='subject',
                            survey_id=survey_id, cast_dtype=False)
    redcap_df = redcap_df.rename(columns={'record_id': 'subject_id',
                                        'old_record_id': 'old_subject_id',
                                        # XXX: what is the new convention?
                                        'date_of_birth': 'date_of_birth_subject'})

    # filter out incomplete rows
    redcap_df = redcap_df.astype({'subject_complete': 'int'})
    redcap_df = redcap_df[redcap_df['subject_complete'] == 2]

    redcap_df = redcap_df[~pd.isna(redcap_df[f'end_time_subject'])]
    rows_subject, cols_subject = dataframe_to_tuple(
        redcap_df,
        df_columns=['subject_id', 'redcap_event_name',
                    'first_name_birth', 'middle_name_birth',
                    'last_name_birth', 'date_of_birth_subject',
                    'country_of_birth', 'gender_at_birth',
                    'birthplace'])

    return rows_subject, cols_subject, redcap_df


def update_database_table(rows_subject, cols_subject, redcap_df,
                          ssh_args=ssh_args, db_args=rc_db_args) -> None:
    """ Updates subject table in database
    """
    with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
        with psycopg2.connect(**db_args) as conn:

            table_subject = Table('subject', conn)
            rename_subject_ids(table_subject, redcap_df)

            # intended to handle a change in individual's data
            # 1. If a subject_id exists, update the other columns
            #    (for the subject whose subject_id changed, updating will
            #     have no effect)
            # 2. If it does not exist, then insert it as a new row
            table_subject.insert_rows(rows_subject, cols_subject,
                                    on_conflict='update',
                                    conflict_cols=['subject_id'])


def main():
    description = "Script to import subject table from REDCap project. Needs redcap project name and survey number of subject table."
    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        '--project-name', 
        type=str, 
        required=True, 
        help='Name of the REDCap project (string)'
    )

    parser.add_argument(
        '--survey-number', 
        type=int, 
        required=True, 
        help='Survey number for subject table (integer)'
    )

    args = parser.parse_args()
    project_name = args.project_name
    survey_id = args.survey_number

    rows_subject, cols_subject, redcap_df = fetch_subject_table(project_name, survey_id)
    update_database_table(rows_subject, cols_subject, redcap_df)

    # update subject table in FA_study database as well if running for FA project
    if 'fa' in project_name:
        rc_db_args['database'] = 'FA_study'
        update_database_table(rows_subject, cols_subject, redcap_df, db_args=rc_db_args)


if __name__ == '__main__':
    main()