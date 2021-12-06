"""Infer JSON schema from a CSV file."""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

import time
import json
import tempfile
import os
import os.path as op

import numpy as np
import pandas as pd
from pandas.api.types import infer_dtype


def iter_interval(wait=60, exit_after=np.inf):
    """Run redcap service in a loop.

    Exit loop with Ctrl + C

    Parameters
    ----------
    wait : float
        The time to wait in seconds.
    exit_after : float
        The time after which to exit in seconds.
    """
    start_time = start_time_t0 = time.time()
    yield
    while True:
        try:
            time.sleep(1)
            curr_time = time.time()
            time_elapsed = curr_time - start_time
            time_left = wait - time_elapsed
            if time_left < 0:
                start_time = time.time()
                print('')
                yield
                continue
            if curr_time - start_time_t0 > exit_after:
                break
            print(f'Time left: {time_left:2.2f} s', end='\r')
        except KeyboardInterrupt:
            break


def fetch_survey(project, survey_name, survey_id, index=None):
    """Get schema of table from redcap

    Parameters
    ----------
    project : instance of Project
        The project created using pycap.
    survey_name : str
        The name of the survey to export
    survey_id : int
        The survey_id. See under Reports in
        Redap.
    index : str
        The column to set as index.

    Returns
    -------
    df : instance of DataFrame
        The pandas dataframe.
    """
    print(f'Fetching report {survey_name} from Redcap')
    data = project.export_reports(report_id=survey_id)
    if 'error' in data:
        raise ValueError(data['error'])

    # format = 'df' didn't work
    df = pd.DataFrame(data)
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_fname = op.join(temp_dir, survey_name + '.csv')
        df.to_csv(csv_fname, index=False)
        # XXX: read back again so pandas casts data to right type
        df = pd.read_csv(csv_fname)
    print('[Done]')

    df = df.where(pd.notnull(df), None)
    if index is not None:
        df.set_index(index)

    return df

def infer_schema(survey_df, metadata_df):
    """Get schema from dataframe.

    Parameters
    ----------
    survey_df : instance of pd.Dataframe
        The dataframe for which to infer the schema.
    metadata_df : instance of pd.Dataframe
        The metadata dataframe containing information
        about all the columns in all the surveys.
    """
    # pandas to bigquery datatype mapping
    # https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema
    mapping = dict(floating='float', integer='integer', string='string')

    dtypes = survey_df.dtypes.to_dict()
    schema = dict()
    for column in survey_df.columns:

        choice = dict()
        question = ''
        field_type = ''
        if column in metadata_df.index:
            row = metadata_df.loc[column]

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
            print(f'Skipping {column}')

        dtype = infer_dtype(survey_df[column], skipna=True)
        dtype = mapping[dtype]
        if dtype == 'string':
            val = survey_df[column].dropna().iloc[0]
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

    return schema
