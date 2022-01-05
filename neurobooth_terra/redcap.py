"""Infer JSON schema from a CSV file."""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

from datetime import datetime, date
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
            if curr_time - start_time_t0 > exit_after:
                break
            elif time_left < 0:
                start_time = time.time()
                print('')
                yield
                continue
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
        df = df.set_index(index)
        df.index = df.index.astype(str)  # allow mixing int and str record_id

    return df

def _is_series_equal(src_series, target_series):
    """Check equality of two series by casting dtype when necessary."""

    for col_index, target_item in target_series.iteritems():
        src_item = src_series.loc[col_index]
        if isinstance(target_item, date):  # ignore datetime column
            continue
        elif type(target_item) != type(src_item):
            src_item = np.array([src_item]).astype(type(target_item))[0]
        if target_item != src_item:
            return False
    return True


def combine_indicator_columns(df, src_cols, target_col):
    """Combine indicator columns into categorical variable.

    Parameters
    ----------
    df : pd.Dataframe
        The dataframe
    src_cols : dict
        The names of the columns are keys and the value it
        gets is the value. Example: dict(race___1=1, race___2=2).
    target_col : str
        The name of the target column to create
    """
    arr = None
    for col_name, val in src_cols.items():
        if arr is None:
            arr = np.zeros_like(df[col_name])
        arr[df[col_name] == 1.] = val
    df[target_col] = arr
    df = df.drop(src_cols.keys(), axis=1)
    return df


def dataframe_to_tuple(df, column_names, fixed_columns=None):
    """Dataframe to tuple.

    Parameters
    ----------
    df : instance of pd.Dataframe
        The dataframe whose index is record_id
    df_columns : list of str
        The column names of the dataframe to process. The column_name
        'record_id' is special and inserts the record_id index.
    fixed_columns : dict
        The columns that have fixed values. E.g., dict(study_id=study1) makes
        all the rows of column study_id to have value study1

    Returns
    -------
    rows : list (n_rows,) of tuples
        A list with each row having the columns of dataframe.
    cols : list of str
        Ordered list of columns in which the tuple entries are added
    """
    if fixed_columns is None:
        fixed_columns = dict()

    rows = list()
    for record_id, df_row in df.iterrows():

        row = list()
        for column_name in column_names:
            if column_name == 'record_id':
                row.append(record_id)
            else:
                row.append(df_row[column_name])

        for column_name in fixed_columns:
            row.append(fixed_columns[column_name])

        rows.append(tuple(row))

    cols = column_names + list(fixed_columns.keys())
    if 'record_id' in cols:
        cols[cols.index('record_id')] = 'subject_id'
    if 'redcap_event_name' in cols:
        cols[cols.index('redcap_event_name')] = 'event_name'
    return rows, cols


def compare_dataframes(src_df, target_df):
    """Compare dataframes.

    Parameters
    ----------
    src_df : pandas dataframe
        The source dataframe whose changes are printed.
    target_df : pandas dataframe
        The target dataframe that is compared against.
    """
    # print extra columns in src_df
    src_columns = set(src_df.columns)
    target_columns = set(target_df.columns)
    common_columns = src_columns.intersection(target_columns)
    print(f'extra columns: {src_columns - common_columns}')

    # differences in rows ignoring extra columns
    print('Changed rows')
    src_df = src_df[common_columns]
    target_df = target_df[common_columns]
    for index, src_row in src_df.iterrows():
        if index not in target_df.index:
            print(src_row[common_columns])
            print('')
            continue
        target_row = target_df.loc[index]

        # if not src_row[target_columns].equals(target_row):
        if not _is_series_equal(src_row, target_row):
            print(src_row[common_columns])
            print('')
            print(target_row)
            print('')
            print('')


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
