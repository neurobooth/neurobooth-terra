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
# Next, we fetch the metadata table. This table is the master table
# that contains columns and their informations. It can be used to infer
# information about the columns: example, what choices are available for a
# particular question.

print('Fetching metadata ...')
metadata = project.export_metadata(format='df')
metadata_fields = ['field_label', 'form_name', 'section_header',
                   'field_type', 'select_choices_or_calculations',
                   'required_field', 'matrix_group_name', 'field_annotation']
metadata = metadata[metadata_fields]
metadata.to_csv('data_dictionary.csv')
print('[Done]')

import pandas as pd

metadata.rename({'form_name': 'redcap_form_name'}, axis=1, inplace=True)
# metadata = metadata[metadata.redcap_form_name.isin(
#    ['subject', 'participant_and_consent_information', 'demograph'])]

for column in ['section_header', 'field_label']:
    metadata[column] = metadata[column].apply(
        lambda x : x.strip('\n') if isinstance(x, str) else x
    )

# FOI should be list
# rename codes FOI, DB, and T for human_obs_data

def extract_info(s):
    field_annot = s['field_annotation']
    if pd.isna(field_annot):
        return s

    fields = field_annot.split(' ')
    for field in fields:
        if not field.startswith('@'):
            if '-' in field:
                field_name, field_value = field.split('-')
                s[field_name] = field_value
            else:
                msg = f'field_annotation reads: {field_annot}'
                s['error'] = msg
                warn(msg)
    return s

# feature of interest
metadata = metadata.apply(extract_info, axis=1)

is_descriptive = metadata['field_type'] == 'descriptive'
metadata['redcap_form_description'] = metadata['field_label']
metadata['redcap_form_description'][~is_descriptive] = None

metadata['question'] = metadata['field_label']
metadata['question'][is_descriptive] = None

# copy first section header of matrix into rest and concatenate with
# question
metadata_groups = metadata.groupby(by='matrix_group_name')
metadata['section_header'] = metadata_groups['section_header'].transform(
    lambda s: s.fillna(method='ffill'))
is_group = ~pd.isna(metadata['section_header'])
metadata['question'][is_group] = (metadata['section_header'][is_group] +
                                  metadata['question'][is_group])

metadata.to_csv('data_dictionary_modified.csv')

rows_metadata, cols_metadata = dataframe_to_tuple(
    metadata, df_columns=['redcap_form_name'],
    index_column='field_name'
)

with SSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(port=tunnel.local_bind_port,
                          host=tunnel.local_bind_host, **db_args) as conn:
        table_metadata = Table('human_obs_data', conn)
        table_metadata.insert_rows(rows_metadata, cols_metadata,
                                   on_conflict='update')
