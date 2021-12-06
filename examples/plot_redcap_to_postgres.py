"""
======================================
Ingest table from Redcap into Postgres
======================================

This example demonstrates how to create table from Redcap.
"""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import os

from redcap import Project, RedcapError
from neurobooth_terra.redcap import (fetch_survey, iter_interval,
                                     compare_dataframes)

import psycopg2
from sshtunnel import SSHTunnelForwarder

from neurobooth_terra import Table, create_table, drop_table

# %%
# The ssh arguments and connection arguments

ssh_args = dict(
        ssh_address_or_host='neurodoor.nmr.mgh.harvard.edu',
        ssh_username='mj513',
        ssh_config_file='~/.ssh/config',
        ssh_pkey='~/.ssh/id_rsa',
        remote_bind_address=('192.168.100.1', 5432),
        local_bind_address=('localhost', 6543)
)

db_args = dict(
    database='neurobooth', user='neuroboother', password='neuroboothrocks',
    # host='localhost'
)

# %%
# Let us first define the surveys and their survey IDs that we want to fetch.
# This information can be found on Redcap. To fetch Redcap data, you will
# also need to define the NEUROBOOTH_REDCAP_TOKEN environment variable.
# You will need to request for the Redcap API token from Redcap interface.

survey_ids = {'consent': 84349, 'contact': 84427, 'demographics': 84429,
              'clinical': 84431, 'falls': 85031, 'subject': 96398}
survey_ids = {'subject': 96397, 'consent': 96398}

URL = 'https://redcap.partners.org/redcap/api/'
API_KEY = os.environ.get('NEUROBOOTH_REDCAP_TOKEN')

if API_KEY is None:
    raise ValueError('Please define the environment variable NEUROBOOTH_REDCAP_TOKEN first')

project = Project(URL, API_KEY, lazy=True)

# %%
# Next, we fetch the metadata table. This table is the master table
# that contains columns and their informations. It can be used to infer
# information about the columns: example, what choices are available for a
# particular question.

print('Fetching metadata ...')
metadata = project.export_metadata(format='df')
metadata_fields = ['field_label', 'form_name', 'section_header',
                   'field_type', 'select_choices_or_calculations',
                   'required_field']
metadata = metadata[metadata_fields]
# metadata.to_csv(op.join(data_dir, 'data_dictionary.csv'), index=False)
print('[Done]')

# %%
# Finally, we loop over the surveys and collect them.
import pandas as pd
import hashlib

# TODO: add test for iter_interval
for _ in iter_interval(wait=5, exit_after=2):

    dfs = dict()
    for survey_name, survey_id in survey_ids.items():
        df = fetch_survey(project, survey_name, survey_id, index='record_id')
        # convert NaN to None for psycopg2
        dfs[survey_name] = df

    # Now, we will prepare the contents of the subject table in postgres
    drop_rows = pd.isna(dfs['subject']['first_name_birth'])
    drop_record_ids = dfs['subject'].index[drop_rows]

    dfs['subject'] = dfs['subject'].drop(drop_record_ids)
    dfs['consent'] = dfs['consent'].drop(drop_record_ids, errors='ignore')

    rows_subject = list()
    for record_id, df_row in dfs['subject'].iterrows():

        mainak_hash = 'hash'

        rows_subject.append((record_id,
                             df_row['first_name_birth'],
                             df_row['middle_name_birth'],
                             df_row['last_name_birth'],
                             df_row['date_of_birth'],
                             df_row['country_of_birth'],
                             df_row['gender_at_birth'],
                             df_row['birthplace']))

    rows_consent = list()
    for record_id, df_row in dfs['consent'].iterrows():
        rows_consent.append((record_id,
                            'study1',  # study_id
                            df_row['redcap_event_name'],
                            'Neuroboother',  # staff_id
                            'REDCAP',  # application_id
                            'MGH',  # site_id
                            # None, # date (missing)
                            df_row['educate_clinicians'],
                            df_row['educate_clinicians_initials'],
                            # bool(df_row['future_research_consent_adult'])
        ))

    for row_subject in rows_subject[:5]:
        print(row_subject)

    # Then we insert the rows in this table
    cols_subject = ['subject_id', 'first_name_birth', 'middle_name_birth',
                    'last_name_birth', 'date_of_birth', 'country_of_birth',
                    'gender_at_birth', 'birthplace']
    cols_consent = ['subject_id', 'study_id', 'event_name',
                    'staff_id', 'application_id', 'site_id',
                    'educate_clinicians', 'educate_clinicians_initials']
    with SSHTunnelForwarder(**ssh_args) as tunnel:
        with psycopg2.connect(port=tunnel.local_bind_port,
                              host=tunnel.local_bind_host, **db_args) as conn:

            table_subject = Table('subject', conn)
            table_consent = Table('consent', conn)

            df_subject_db = table_subject.query()
            df_subject_db = df_subject_db.drop('guid', axis=1)  # extra column in DB
            df_consent_db = table_consent.query()
            df_consent_db = df_consent_db.drop(
                ['study_id', 'staff_id', 'application_id', 'site_id',
                 'future_research_consent_adult', 'educate_clinicians_adults',
                 'educate_clinicians_initials_adult', 'event_name'], axis=1)
            compare_dataframes(dfs['subject'], df_subject_db)
            compare_dataframes(dfs['consent'], df_consent_db)

            table_subject.insert_rows(rows_subject, cols_subject,
                                      on_conflict='update')
            table_consent.insert_rows(rows_consent, cols_consent,
                                      on_conflict='update')

# %%
# We will drop our tables if they already exist
# this is just for convenience so we can re-run this script
# and create a new mock subject table and consent table to test our script
# drop_table('subject', conn)
# drop_table('consent', conn)

# table_id = 'subject'
# datatypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)',
#              'date', 'VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)']
# table_subject = create_table(table_id, conn, cols_subject, datatypes)

# table_id = 'consent'
# datatypes = ['VARCHAR (255)'] * len(cols_consent)
# table_consent = create_table(table_id, conn, cols_consent, datatypes)

# %%
# Let's do a query to check that the content is in there
# print(table_subject.query())
# print(table_consent.query())
