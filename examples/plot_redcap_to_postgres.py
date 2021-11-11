"""
======================================
Ingest table from Redcap into Postgres
======================================

This example demonstrates how to create table from Redcap.
"""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import os
import os.path as op

import json
from redcap import Project, RedcapError

import psycopg2
from neurobooth_terra import Table, create_table, drop_table
from neurobooth_terra.ingest_redcap import fetch_survey, infer_schema

# %%
# Let us first define the surveys and their survey IDs that we want to fetch.
# This information can be found on Redcap. To fetch Redcap data, you will
# also need to define the NEUROBOOTH_REDCAP_TOKEN environment variable.
# You will need to request for the Redcap API token from Redcap interface.

survey_ids = {'consent': 84349, 'contact': 84427, 'demographics': 84429,
              'clinical': 84431, 'falls': 85031, 'subject': 84426}
survey_ids = {'subject': 84426, 'consent': 84349}

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
# Finally, we loop over the surveys and print out the schema.
import pandas as pd

json_schema = dict()
dfs = dict()
for survey_name, survey_id in survey_ids.items():
    df = fetch_survey(project, survey_name, survey_id)
    json_schema[survey_name] = infer_schema(df, metadata)
    # convert to None for psycopg2
    df = df.where(pd.notnull(df), None)
    dfs[survey_name] = df
print(json.dumps(json_schema[survey_name], indent=4, sort_keys=True))

# %%
# Now, we will add the consent table to the subject table so we can
# match subjects based on record_id

df_joined = dfs['subject'].join(dfs['consent'], rsuffix='consent')

# %%
# Now, we will prepare the subject table in postgres

import hashlib

rows_subject = list()
rows_consent = list()

subject_ids = list()

for df_row in df_joined.iterrows():
    df_row = df_row[1]

    # need at least name to add to table
    if df_row['first_name_birth'] is None:
        continue

    subject_id = df_row['first_name_birth'] + df_row['last_name_birth']
    subject_id = hashlib.md5(subject_id.encode('ascii')).hexdigest()

    # XXX: hack, why are there duplicate subjects?
    if subject_id in subject_ids:
        continue
    subject_ids.append(subject_id)

    rows_subject.append((subject_id,
                        df_row['first_name_birth'],
                        df_row['middle_name_birth'],
                        df_row['last_name_birth'],
                        df_row['date_of_birth'],
                        df_row['country_of_birth'],
                        df_row['gender_at_birth'],
                        df_row['birthplace']))
    rows_consent.append((subject_id,
                        'study1',  # study_id
                        'Neuroboother',  # staff_id
                        'REDCAP',  # application_id
                        'MGH',  # site_id
                        # None, # date (missing)
                        # df_row['educate_clinicians_adults'],
                        # df_row['educate_clinicians_initials_adult'],
                        # bool(df_row['future_research_consent_adult'])
    ))

# %%
# Now, we will first create a connection to the database
connect_str = ("dbname='neurobooth' user='neuroboother' host='localhost' "
               "password='neuroboothrocks'")

conn = psycopg2.connect(connect_str)

# %%
# We will drop our tables if they already exist
# this is just for convenience so we can re-run this script
# and create a new mock subject table and consent table to test our script
drop_table('subject', conn)
drop_table('consent', conn)

table_id = 'subject'
cols_subject = ['subject_id', 'first_name_birth', 'middle_name_birth',
           'last_name_birth', 'date_of_birth', 'country_of_birth',
           'gender_at_birth', 'birthplace']
datatypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)',
             'date', 'VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)']
table_subject = create_table(table_id, conn, cols_subject, datatypes)

table_id = 'consent'
cols_consent = ['subject_id', 'study_id', 'staff_id', 'application_id',
                'site_id']
datatypes = ['VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)', 'VARCHAR (255)',
             'VARCHAR (255)']
table_consent = create_table(table_id, conn, cols_consent, datatypes)

# %%
# Then we insert the rows in this table
table_subject.insert_rows(rows_subject, cols_subject)
table_consent.insert_rows(rows_consent, cols_consent)
