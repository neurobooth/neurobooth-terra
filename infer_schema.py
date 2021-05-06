"""Infer JSON schema from a CSV file."""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import json

import os.path as op
import pandas as pd

data_dir = ('/Users/mainak/Dropbox (Partners HealthCare)/neurobooth_data/'
            'register/')
csv_fname = op.join(data_dir,
                    'Neurobooth-ConsentExport_DATA_2021-05-05_1409.csv')

# pandas to bigquery datatype mapping
# https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema
mapping = dict(float64='FLOAT', str='STRING', int64='INTEGER')

df = pd.read_csv(csv_fname)
dtypes = df.dtypes.to_dict()
for k, v in dtypes.items():
    dtypes[k] = v.name
    if v.name == 'object':
        dtypes[k] = 'str'
    dtypes[k] = mapping[dtypes[k]]

print(json.dumps(dtypes, indent=4, sort_keys=True))
