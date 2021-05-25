"""Infer JSON schema from a CSV file."""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import json
import os
import os.path as op

import pandas as pd
from pandas.api.types import infer_dtype

from redcap import Project, RedcapError

data_dir = ('/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/'
            'register/')
schema_fname = op.join(data_dir, 'schema.json')
survey_ids = {'consent': 84349, 'contact': 84427, 'demographics': 84429}

URL = 'https://redcap.partners.org/redcap/api/'
API_KEY = os.environ.get('NEUROBOOTH_REDCAP_TOKEN')
metadata_fields = ['field_label', 'form_name', 'section_header',
                   'field_type', 'select_choices_or_calculations',
                   'required_field']

if API_KEY is None:
    raise ValueError('Please define the environment variable NEUROBOOTH_REDCAP_TOKEN first')

project = Project(URL, API_KEY, lazy=True)
print('Fetching metadata ...')
metadata = project.export_metadata(format='df')
metadata = metadata[metadata_fields]
metadata.to_csv(op.join(data_dir, 'data_dictionary.csv'), index=False)
print('[Done]')

# pandas to bigquery datatype mapping
# https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema
mapping = dict(floating='float', integer='integer', string='string')

json_schema = dict()
for survey_name, survey_id in survey_ids.items():
    print(f'Fetching report {survey_name} from Redcap')
    data = project.export_reports(report_id=survey_id)
    # format = 'df' didn't work
    df = pd.DataFrame(data)
    df.to_csv(op.join(data_dir, survey_name + '.csv'), index=False)
    print('[Done]')

    dtypes = df.dtypes.to_dict()
    schema = dict()
    for column in df.columns:

        choice = dict()
        question = ''
        field_type = ''
        if column in metadata.index:
            row = metadata.loc[column]

            question = row['field_label']
            if question.startswith('<'): # html
                question = ''

            field_type = row['field_type']

            choices = row['select_choices_or_calculations']
            if not pd.isnull(choices):
                choices = choices.split('|')
                for c in choices:
                    k, v = c.strip().split(', ')
                    choice[k] = v
        else:
            print(f'Skipping {survey_name}::{column}')

        dtype = infer_dtype(df[column], skipna=True)
        dtype = mapping[dtype]
        if dtype == 'string':
            val = df[column].dropna().iloc[0]
        if column.startswith('date'):  # hardcode for now
            dtype = 'datetime'

        schema[column] = {
            'name': column,
            'type': dtype,
            'mode': 'NULLABLE',
            'choices': choice,
            'question': question,
            'field_type': field_type
        }

    json_schema[survey_name] = schema

json_schema = json.dumps(json_schema, indent=4, sort_keys=True)
print(json_schema)
with open(schema_fname, 'w') as fp:
    fp.write(json_schema)
