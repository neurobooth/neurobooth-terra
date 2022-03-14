"""Functions to parse redcap surveys."""

# Authors: Mainak Jas <mjas@mgh.harvard.edu>

from datetime import datetime, date
from warnings import warn
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
    arr = [list() for _ in range(df.shape[0])]
    for col_name, val in src_cols.items():
        for idx, this_element in enumerate(df[col_name]):
            if this_element == 1.:
                arr[idx].append(int(val))
    df[target_col] = arr
    df.drop(src_cols.keys(), axis=1, inplace=True)
    return df


def extract_field_annotation(s):
    """Extract the field annotation and create new columns for them

    Annotations are structured in the format:
    FOI-visual DB-y FOI-motor

    Returns
    -------
    s : pandas series object
        The pandas series with error and columns for field annotation added.
    """
    field_annot = s['field_annotation']
    if pd.isna(field_annot):
        return s

    fields = field_annot.split(' ')
    fois = list()
    for field in fields:
        if field.startswith('@'):
            continue

        try:
            field_name, field_value = field.split('-')
            if field_name == 'FOI':
                fois.append(field_value)
            else:
                s[field_name] = field_value
            s['error'] = ''
        except Exception as e:
            msg = f'field_annotation reads: {field_annot}'
            s['error'] = msg
            warn(msg)
    s['FOI'] = fois
    return s


def get_response_array(s):
    """Get response array."""
    choices = s['select_choices_or_calculations']
    if s['field_type'] not in ['radio', 'checkbox']:
        return s

    response_array = dict()
    choices = choices.split('|')
    for c in choices:
        k, v = c.strip().split(', ', maxsplit=1)
        response_array[k] = v
    s['response_array'] = response_array
    return s


def map_dtypes(s):
    """Map data types from Redcap to database and Python.

    Returns
    -------
    s : pandas series object
        The pandas series object containing new entries database_dtype
        and python_dtype
    """
    dtype_mapping = {'calc': 'double precision', 'checkbox': 'smallint[]',
                     'dropdown': 'smallint', 'notes': 'text',
                     'radio': 'smallint', 'yesno': 'boolean',
                     'file': 'varchar(255)'}
    text_dtype_mapping = {'date_mdy': 'date', 'email': 'varchar(255)',
                          'datetime_seconds_ymd': 'timestamp',
                          'datetime_seconds_mdy': 'timestamp',
                          'mrn_6d': 'integer', 'number': 'integer',
                          'phone': 'varchar(15)'}
    python_dtype_mapping = {'smallint[]': 'list',
                            'boolean': 'bool',
                            'text': 'str', 'varchar(255)': 'str',
                            'timestamp': 'str', 'date': 'str',
                            'datetime': 'str',
                            'double precision': 'float64',
                            'smallint': 'Int64', 'bigint': 'Int64',
                            'integer': 'Int64', 'varchar(15)': 'str',
                            'file': 'str'}

    redcap_dtype = s['field_type']
    text_validation = s['text_validation_type_or_show_slider_number']

    if pd.isna(redcap_dtype) or redcap_dtype in ['descriptive']:
        return s

    if redcap_dtype in dtype_mapping:
        s['database_dtype'] = dtype_mapping[redcap_dtype]
    elif redcap_dtype == 'text':
        s['database_dtype'] = text_dtype_mapping.get(text_validation, 'text')

    s['python_dtype'] = python_dtype_mapping[s['database_dtype']]

    return s


def get_tables_structure(metadata, include_surveys=None):
    """Get the column names and datatypes for the tables.

    Returns
    -------
    table_infos : dict
        Dictionary with keys as table names and each table having the following
        entries: columns, dtypes, indicator_columns, and python_columns,
        python_dtypes (for casting columns to the right Python data type).
    """
    metadata_by_form = metadata[np.any(
        [metadata['in_database'] == 'y', metadata['database_dtype'].notnull()],
        axis=0)]
    metadata_by_form = metadata_by_form.groupby('redcap_form_name')

    special_columns = {
        'columns': ['subject_id',
                    # integer for first instance is null and for
                    # subsequent instance is integer starting at 1 and should
                    # correspond to integer
                    # in redcap_event_name (if completed once per visit)
                    'redcap_repeat_instance',
                    'redcap_event_name',
                    # XXX: don't add in database?
                    'redcap_repeat_instrument'],
        'dtypes': ['varchar(255)', 'integer', 'varchar(255)', 'varchar(255)'],
        'python_dtypes': ['str', 'Int64', 'str', 'str']
    }

    table_infos = dict()
    for form_name, metadata_form in metadata_by_form:
        if form_name == 'subject':  # subject table is special
            continue

        table_infos[form_name] = {
            'columns': list(), 'dtypes': list(), 'python_dtypes': list(),
            'indicator_columns': list()
        }
        for index, row in metadata_form.iterrows():

            if row['database_dtype'] == 'smallint[]':  # checkbox column
                table_infos[form_name]['indicator_columns'].append(index)
            else:
                table_infos[form_name]['columns'].append(index)
                table_infos[form_name]['dtypes'].append(row['database_dtype'])
                table_infos[form_name]['python_dtypes'].append(row['python_dtype'])

        for meta_name in special_columns:
            table_infos[form_name][meta_name].extend(special_columns[meta_name])

        table_infos[form_name]['columns'].append(f'{form_name}_complete')
        table_infos[form_name]['dtypes'].append('integer')
        table_infos[form_name]['python_dtypes'].append('Int64')

    if include_surveys is not None:
        table_infos = {k: v for (k, v) in table_infos.items() if k in include_surveys}

    return table_infos


def subselect_table_structure(table_info, df_cols):
    """Subselect columns that are in the report."""

    table_info_new = {'columns': list(), 'dtypes': list(),
                      'python_dtypes': list()}
    for col, dtype, python_dtype in zip(table_info['columns'],
                                        table_info['dtypes'],
                                        table_info['python_dtypes']):
        if col in df_cols:
            table_info_new['columns'].append(col)
            table_info_new['dtypes'].append(dtype)
            table_info_new['python_dtypes'].append(python_dtype)
    table_info_new['indicator_columns'] = table_info['indicator_columns']
    return table_info_new


def dataframe_to_tuple(df, df_columns, fixed_columns=None,
                       indicator_columns=None):
    """Dataframe to tuple.

    Parameters
    ----------
    df : instance of pd.Dataframe
        The dataframe whose index is record_id
    df_columns : list of str
        The column names of the dataframe to process. The column_name
        'record_id' is special and inserts the record_id index.
    fixed_columns : dict | None
        The columns that have fixed values. E.g., dict(study_id=study1) makes
        all the rows of column study_id to have value study1
    indicator_columns : list of str | None
        The indicator columns.

    Returns
    -------
    rows : list (n_rows,) of tuples
        A list with each row having the columns of dataframe.
    cols : list of str
        Ordered list of columns in which the tuple entries are added
    """
    if fixed_columns is None:
        fixed_columns = dict()

    if indicator_columns is None:
        indicator_columns = list()

    for indicator_column in indicator_columns:
        mapping = dict()  # {race___1: 1, race___2: 2}
        for col in df.columns:
            if col.startswith(indicator_column):
                mapping[col] = col.split('___')[1]
        if len(mapping) == 0:
            raise ValueError(f'No column found starting with {indicator_column}')
        df = combine_indicator_columns(df, mapping, indicator_column)

    rows = list()
    for index, df_row in df.iterrows():

        row = list()
        for column_name in df_columns:
            row.append(df_row[column_name])
            # XXX: hack, None/nan means missing values in comments.
            if not isinstance(row[-1], list) and \
                    (pd.isna(row[-1]) or row[-1] in ('None', 'nan')):
                row[-1] = None

        for column_name in fixed_columns:
            row.append(fixed_columns[column_name])

        rows.append(tuple(row))

    cols = df_columns + list(fixed_columns.keys())

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
