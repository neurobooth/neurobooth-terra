"""Infer JSON schema from a CSV file."""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import json

import os.path as op
import pandas as pd
from pandas.api.types import infer_dtype

data_dir = ('/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/'
            'register/')
csv_fname = op.join(data_dir,
                    'Neurobooth-ConsentExport_DATA_2021-05-05_1409.csv')

# pandas to bigquery datatype mapping
# https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema
mapping = dict(floating='float', integer='integer', string='string')

df = pd.read_csv(csv_fname, parse_dates=True)
dtypes = df.dtypes.to_dict()
for column in df.columns:
    dtypes[column] = infer_dtype(df[column])
    dtypes[column] = mapping[dtypes[column]]
    if dtypes[column] == 'string':
        val = df[column].dropna().iloc[0]
        try:
            pd.Timestamp(val)
        except:
            dtypes[column] = 'timestamp'

# Convert to fields that SchemaField expects
for k, v in dtypes.items():
    dtypes[k] = {
        "name": v,
        "type": v,
        "mode": "REQUIRED"
    }

print(json.dumps(dtypes, indent=4, sort_keys=True))
