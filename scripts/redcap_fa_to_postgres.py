"""
======================================
Ingest tables from FA REDCap project into Postgres
======================================
"""

from warnings import warn

import pandas as pd
import numpy as np
import datetime

from neurobooth_terra.fixes import OptionalSSHTunnelForwarder
import psycopg2

from config import ssh_args, db_args, fa_project
from neurobooth_terra.redcap import (map_dtypes,
                                     extract_field_annotation,
                                     get_response_array,
                                     get_tables_structure,
                                     dataframe_to_tuple,
                                     fetch_survey,
                                     subselect_table_structure,
                                    )
from neurobooth_terra import Table, create_table, drop_table


survey_ids = {
    'baseline_data': 197330,
    'demographic': 205252,
    'clinical': 213369,
    'visit_dates': 205301,
    'presession_survey': 205302,
    'fahi_8_and_older': 205303,
    'facrhi_5_and_older': 205304,
    'prom_ataxia_short_form': 205305,
    'neurobooth_falls': 205306,
    'neuro_qol_fatigue_short_form': 205307,
    'patient_global_impression_of_change': 205309,
    'activities_of_daily_living': 205310,
    'fars_mfars_functional_staging': 205311,
    'bars_micars_uhdrs': 205312,
    'end_of_visit_details': 205313,
    'study_feedback': 205314,
    'system_usability_scale': 205315,
    }


print('Fetching metadata ...')
metadata = fa_project.export_metadata(format='df') # format_type='df' in latest version of pycap
metadata_fields = ['field_label', 'form_name', 'section_header',
                   'field_type', 'select_choices_or_calculations',
                   'required_field', 'matrix_group_name', 'field_annotation',
                   'text_validation_type_or_show_slider_number']
metadata = metadata[metadata_fields]
metadata.to_csv('fa_data_dictionary.csv')
print('[Done]')


for column in ['section_header', 'field_label']:
    metadata[column] = metadata[column].apply(
        lambda x: x.strip('\n') if isinstance(x, str) else x
    )

metadata = metadata.apply(map_dtypes, axis=1)
metadata = metadata.apply(extract_field_annotation, axis=1)
metadata = metadata.apply(get_response_array, axis=1)
metadata.rename({'form_name': 'redcap_form_name',
                 'FOI': 'feature_of_interest', 'DB': 'in_database',
                 'T': 'database_table_name',
                 'redcap_event_name': 'event_name'}, axis=1, inplace=True)

is_descriptive = metadata['field_type'] == 'descriptive'
metadata['redcap_form_description'] = metadata['field_label']
metadata.loc[~is_descriptive, 'redcap_form_description'] = None

metadata['question'] = metadata['field_label']
metadata.loc[is_descriptive, 'question'] = None

if 'database_table_name' not in metadata.columns:
    metadata['database_table_name'] = np.nan
metadata['database_table_name'] = metadata['database_table_name'].fillna(
    value=metadata['redcap_form_name'])

# copy first section header of matrix into rest
# and concatenate with question
metadata_groups = metadata.groupby(by='matrix_group_name')
metadata['section_header'] = metadata_groups['section_header'].transform(
    lambda s: s.infer_objects().ffill())
is_group = ~pd.isna(metadata['section_header'])
metadata.loc[is_group, 'question'] = (metadata['section_header'][is_group] +
                                  metadata['question'][is_group])

metadata.to_csv('fa_data_dictionary_modified.csv')
table_infos = get_tables_structure(metadata, include_surveys=survey_ids.keys())


# adding last_updated column to data_dictionary
metadata['last_updated'] = datetime.datetime.now()
metadata = metadata.reset_index()
rows_metadata, cols_metadata = dataframe_to_tuple(
    metadata, df_columns=['field_name', 'redcap_form_name',
                          'database_table_name', 'redcap_form_description',
                          'feature_of_interest', 'question', 'response_array',
                          'last_updated']
)

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(**db_args) as conn:

        # Unlike other tables, data_dictionary is not dropped.
        # Instead the rows are only ever updated.
        # This means all the old variables are still retained in
        # the table - latest variables are evident by last_updated
        # timestamp column
        table_metadata = Table('rc_data_dictionary', conn)
        table_metadata.insert_rows(rows_metadata, cols_metadata,
                                   on_conflict='update')

        for table_id, _ in survey_ids.items():
            
            table_info = table_infos[table_id]
            print(f'Overwriting table {table_id}')
            drop_table('rc_' + table_id, conn)

            primary_keys = ['subject_id', 'redcap_event_name']
            table = create_table('rc_' + table_id, conn,
                                 table_info['columns']+table_info['indicator_columns'],
                                 table_info['dtypes']+(['smallint[]']*len(table_info['indicator_columns'])),
                                 primary_key=primary_keys)
            df = fetch_survey(fa_project, survey_name=table_id,
                              survey_id=survey_ids[table_id])
            df = df.rename(columns={'record_id': 'subject_id'})

            report_cols = set([col.split('___')[0] for col in df.columns])
            extra_cols = report_cols - (set(table_info['columns']) |
                                        set(table_info['indicator_columns']))
            if len(extra_cols) > 0:
                raise ValueError(f'Report {table_id} contains ({extra_cols})'
                                 f'that are not found in data dictionary')

            table_info = subselect_table_structure(table_info, df.columns)

            df = df.astype(dict(zip(table_info['columns'],
                                    table_info['python_dtypes'])))
            rows, columns = dataframe_to_tuple(
                df, df_columns=table_info['columns'],
                indicator_columns=table_info['indicator_columns'])

            table.insert_rows(rows, columns)
