"""
======================================
Ingest table from Redcap into Postgres
======================================

This example demonstrates how to create table from Redcap.
"""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import os
from warnings import warn

from redcap import Project, RedcapError
from neurobooth_terra.redcap import (fetch_survey, iter_interval,
                                     compare_dataframes,
                                     combine_indicator_columns,
                                     dataframe_to_tuple, infer_schema)

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

survey_ids = {'subject': 96397, 'consent': 96398, 'demographics': 98294,
              'clinical': 84431}

URL = 'https://redcap.partners.org/redcap/api/'
API_KEY = os.environ.get('NEUROBOOTH_REDCAP_TOKEN')

if API_KEY is None:
    raise ValueError('Please define the environment variable NEUROBOOTH_REDCAP_TOKEN first')

project = Project(URL, API_KEY, lazy=True)

# %%
# Finally, we loop over the surveys and collect them.
import pandas as pd
import hashlib

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
dfs['demographics'] = dfs['demographics'].drop(drop_record_ids, errors='ignore')

dfs['consent'] = dfs['consent'][~pd.isna(dfs['consent']['consent_date'])]
dfs['demographics'] = dfs['demographics'][~pd.isna(dfs['demographics']['end_time_demographics'])]

# Then we insert the rows in this table
rows_subject, cols_subject = dataframe_to_tuple(
    dfs['subject'],
    df_columns=['first_name_birth', 'middle_name_birth',
                'last_name_birth', 'date_of_birth', 'country_of_birth',
                'gender_at_birth', 'birthplace'],
    index_column='record_id')

rows_consent, cols_consent = dataframe_to_tuple(
    dfs['consent'],
    df_columns=['redcap_event_name', 'educate_clinicians',
                'educate_clinicians_initials'],
    fixed_columns=dict(study_id='study1', staff_id='Neuroboother',
                        application_id='REDCAP', site_id='MGH'),
    index_column='record_id'
)

rows_demographics, cols_demographics = dataframe_to_tuple(
    dfs['demographics'],
    df_columns=['redcap_event_name', 'gender', 'ethnicity',
                'handedness', 'race'],
    fixed_columns=dict(study_id='study1', application_id='REDCAP'),
    indicator_columns=['race', 'ancestry_cateogry'],  # health_history?
    index_column='record_id'
)

for row_subject in rows_subject[:5]:
    print(row_subject)

with SSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                            host=tunnel.local_bind_host, **db_args) as conn:

        table_subject = Table('subject', conn)
        table_consent = Table('consent', conn)
        table_demographics = Table('demographics', conn)

        df_subject_db = table_subject.query()
        df_consent_db = table_consent.query()
        compare_dataframes(dfs['subject'], df_subject_db)
        # compare_dataframes(dfs['consent'], df_consent_db)

        table_subject.insert_rows(rows_subject, cols_subject,
                                    on_conflict='update')
        table_consent.insert_rows(rows_consent, cols_consent,
                                    on_conflict='update')
        table_demographics.insert_rows(rows_demographics, cols_demographics,
                                        on_conflict='update')
