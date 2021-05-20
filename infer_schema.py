"""Infer JSON schema from a CSV file."""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import json

import os.path as op
import pandas as pd
from pandas.api.types import infer_dtype

data_dir = ('/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/'
            'register/')
schema_fname = op.join(data_dir, 'schema.json')
csv_table = {
    'consent': 'Neurobooth-ConsentScreeningAndC_DATA_2021-05-18_1213.csv',
    'contact': 'Neurobooth-ContactInfo_DATA_2021-05-18_1215.csv',
    'demographics': 'Neurobooth-Demographics_DATA_2021-05-18_1217.csv'
}

# pandas to bigquery datatype mapping
# https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema
mapping = dict(floating='float', integer='integer', string='string')

json_schema = dict()
for table_name, csv_fname in csv_table.items():
    csv_fname_full = op.join(data_dir, csv_fname)
    df = pd.read_csv(csv_fname_full, parse_dates=True)

    dtypes = df.dtypes.to_dict()
    for column in df.columns:
        dtypes[column] = infer_dtype(df[column], skipna=True)
        dtypes[column] = mapping[dtypes[column]]
        if dtypes[column] == 'string':
            val = df[column].dropna().iloc[0]
        if column.startswith('date'):  # hardcode for now
            dtypes[column] = 'datetime'

    # Convert to fields that SchemaField expects
    for k, v in dtypes.items():
        dtypes[k] = {
            "name": k,
            "type": v,
            "mode": "NULLABLE"
        }
    json_schema[table_name] = dtypes

json_schema = json.dumps(json_schema, indent=4, sort_keys=True)
print(json_schema)
with open(schema_fname, 'w') as fp:
    fp.write(json_schema)

