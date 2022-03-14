"""
======================================
Ingest table from Redcap into Postgres
======================================

This example demonstrates how to create table from Redcap.
"""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import os
from warnings import warn

import numpy as np
import pandas as pd  # version > 1.4.0

from redcap import Project, RedcapError

from neurobooth_terra.redcap import (fetch_survey, dataframe_to_tuple,
                                     extract_field_annotation, map_dtypes,
                                     get_tables_structure,
                                     subselect_table_structure,
                                     get_response_array)
from neurobooth_terra import create_table, drop_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder

import psycopg2

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

survey_ids = {'subject': 99915,
              'consent': 96398,
              'contact': 99916,
              'demograph': 99917,  # -- first_language has mixed datatypes
              'clinical': 99918,
              'visit_dates': 99919,  # cannot deal with arms
              'neurobooth_falls': 99920,
              'neuro_qol_ue_short_form': 99921,
              'neuro_qol_le_short_form': 99922,
              'neuro_qol_cognitive_function_short_form': 99923,
              'neuro_qol_stigma_short_form': 99924,
              'neuro_qol_ability_to_participate_in_social_roles_a': 99925,
              'neuro_qol_satisfaction_with_social_roles_and_activ': 99926,
              'neuro_qol_emotional_and_behavioral_dyscontrol_shor': 99928,
              'neuro_qol_positive_affect_and_wellbeing_short_form': 99929,
              'neuro_qol_sleep_disturbance_short_form': 99931,
              'cpib': 99932,
              'chief_short_form': 99933,
              'neurobooth_vision_prom_ataxia': 99934,
              'promis_10': 99935,
              'system_usability_scale': 99936,
              'study_feedback': 99937,
              'neuro_qol_depression_short_form': 99938}

# subject table updating (old subject ID using first name, last name, dob)
# table column mapping
# metadata table (response_array remains)
# how to rename subject IDs (cascading), within database + outside database

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
                   'required_field', 'matrix_group_name', 'field_annotation',
                   'text_validation_type_or_show_slider_number']
metadata = metadata[metadata_fields]
metadata.to_csv('data_dictionary.csv')
print('[Done]')

# metadata = metadata[metadata.redcap_form_name.isin(
#    ['subject', 'participant_and_consent_information', 'demograph'])]

for column in ['section_header', 'field_label']:
    metadata[column] = metadata[column].apply(
        lambda x : x.strip('\n') if isinstance(x, str) else x
    )

# feature of interest
metadata = metadata.apply(map_dtypes, axis=1)
metadata = metadata.apply(extract_field_annotation, axis=1)
metadata = metadata.apply(get_response_array, axis=1)
metadata.rename({'form_name': 'redcap_form_name',
                 'FOI': 'feature_of_interest', 'DB': 'in_database',
                 'T': 'database_table_name',
                 'redcap_event_name': 'event_name'}, axis=1, inplace=True)

is_descriptive = metadata['field_type'] == 'descriptive'
metadata['redcap_form_description'] = metadata['field_label']
metadata['redcap_form_description'][~is_descriptive] = None

metadata['question'] = metadata['field_label']
metadata['question'][is_descriptive] = None

metadata['database_table_name'] = metadata['database_table_name'].fillna(
    value=metadata['redcap_form_name'])

# copy first section header of matrix into rest and concatenate with
# question
metadata_groups = metadata.groupby(by='matrix_group_name')
metadata['section_header'] = metadata_groups['section_header'].transform(
    lambda s: s.fillna(method='ffill'))
is_group = ~pd.isna(metadata['section_header'])
metadata['question'][is_group] = (metadata['section_header'][is_group] +
                                  metadata['question'][is_group])

metadata.to_csv('data_dictionary_modified.csv')

table_infos = get_tables_structure(metadata, include_surveys=survey_ids.keys())

metadata = metadata.reset_index()
rows_metadata, cols_metadata = dataframe_to_tuple(
    metadata, df_columns=['field_name', 'redcap_form_name',
                          'database_table_name', 'redcap_form_description',
                          'feature_of_interest', 'question', 'response_array']
)

df = fetch_survey(project, survey_name='subject',
                  survey_id=survey_ids['subject'])
df = df.rename(columns={'record_id': 'subject_id'})
df = df[~pd.isna(df[f'end_time_subject'])]
rows_subject, cols_subject = dataframe_to_tuple(
    df,
    df_columns=['subject_id', 'first_name_birth', 'middle_name_birth',
                'last_name_birth', 'date_of_birth', 'country_of_birth',
                'gender_at_birth', 'birthplace'])

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:

        table_metadata = Table('human_obs_data', conn)
        table_metadata.insert_rows(rows_metadata, cols_metadata,
                                   on_conflict='update')

        table_subject = Table('subject', conn)
        table_subject.insert_rows(rows_subject, cols_subject,
                                  on_conflict='update')

        for table_id, table_info in table_infos.items():
            print(f'Overwriting table {table_id}')
            drop_table(table_id, conn)
            table = create_table(table_id, conn, table_info['columns'],
                                 table_info['dtypes'],
                                 primary_key=['subject_id', 'redcap_event_name'])
            df = fetch_survey(project, survey_name=table_id,
                              survey_id=survey_ids[table_id])
            df = df.rename(columns={'record_id': 'subject_id'})

            # XXX: not consistent.
            complete_col = [col for col in df.columns if
                            col.endswith('complete')]
            if len(complete_col) == 0:
                warn(f'Skipping {table_id} because of missing complete col')
                continue
            df = df[df[complete_col[0]] == 2]

            report_cols = set([col.split('___')[0] for col in df.columns])
            extra_cols = report_cols - (set(table_info['columns']) |
                                        set(table_info['indicator_columns']))
            if len(extra_cols) > 0:
                raise ValueError(f'Report {table_id} contains ({extra_cols})'
                                 f'that are not found in data dictionary')

            table_info = subselect_table_structure(table_info, df.columns)
            df = df.astype(dict(zip(table_info['columns'],
                                    table_info['python_dtypes'])))
            rows, columns = dataframe_to_tuple(
                df, df_columns=table_info['columns'],
                indicator_columns=table_info['indicator_columns'])

            table.insert_rows(rows, columns)
