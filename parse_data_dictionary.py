"""Parse data dictionary to get critical info."""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import os.path as op
import pandas as pd

data_dir = '/Users/mainak/Dropbox (Partners HealthCare)/Neurobooth Redcap Data'
data_dictionary_fname = op.join(data_dir,
                                'Neurobooth_DataDictionary_2021-05-18.csv')

field = 'consent_age_range'
df = pd.read_csv(data_dictionary_fname)
is_required = df[df['Variable / Field Name'] == field]['Required Field?']
print(is_required)

# XXX: div in Field Label