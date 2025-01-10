"""
======================================
Ingest table from Redcap into Postgres
======================================

This example demonstrates how to create table from Redcap.
"""

# Authors: Mainak Jas <mjas@harvard.mgh.edu>

import os
from warnings import warn

import numpy as np
import pandas as pd

from redcap import RedcapError

from neurobooth_terra.redcap import (fetch_survey, dataframe_to_tuple,
                                     extract_field_annotation, map_dtypes,
                                     get_tables_structure,
                                     subselect_table_structure,
                                     get_response_array)
from neurobooth_terra import create_table, drop_table
from neurobooth_terra.fixes import OptionalSSHTunnelForwarder

import psycopg2

from neurobooth_terra import Table, create_table, drop_table
from neurobooth_terra.views.views import create_views, drop_views
from config import ssh_args, db_args, project

# %%
# Let us first define the surveys and their survey IDs that we want to fetch.
# This information can be found on Redcap. To fetch Redcap data, you will
# also need to define the NEUROBOOTH_REDCAP_TOKEN environment variable.
# You will need to request for the Redcap API token from Redcap interface.

# The name of the surveys and their ids should match how they are defined in
# how redcap metadata data_dictionary.
# Table names and how they're referred to in the dictionary are not always the
# same:
# for example pgic_v1 for "patient_global_impression_of_change_first_visit"
survey_ids = {
    'consent': 99891,
    'consent_nih_sca': 99896,
    'contact': 99916,
    'demographic': 99917,
    'clinical': 99918,
    'visit_dates': 99919,
    'neurobooth_falls': 99920,
    'neuro_qol_ue_short_form': 99921,
    'neuro_qol_le_short_form': 99922,
    'neuro_qol_cognitive_function_short_form': 99923,
    'neuro_qol_stigma_short_form': 99924,
    'neuro_qol_participate_social_roles_short_form': 99925,
    'neuro_qol_satisfaction_social_roles_short_form': 99926,
    'neuro_qol_anxiety_short_form': 99927,
    'neuro_qol_emotional_dyscontrol_short_form': 99928,
    'neuro_qol_positive_affect_and_wellbeing_short_form': 99929,
    'neuro_qol_fatigue_short_form': 99930,
    'neuro_qol_sleep_disturbance_short_form': 99931,
    'communicative_participation_item_bank': 99932,
    'chief_short_form': 99933,
    'promis_10': 99935,
    'system_usability_scale': 99936,
    'study_feedback': 99937,
    'neuro_qol_depression_short_form': 99938,
    'neurobooth_vision_prom_ataxia': 99934,
    'prom_ataxia': 102336,
    'prom_ataxia_short_form': 142151,
    'dysarthria_impact_scale': 102384,
    'ataxia_pd_scales': 103620,
    'alsfrs': 148735,
    'participant_and_consent_information': 124062,
    'handedness_questionnaire': 123490,
    'visual_activities_questionnaire': 139136,
    'end_of_visit_details': 127247,
    'pgic_v1': 175962, # patient_global_impression_of_change_first_visit
    'pgic_followup_visits': 175964, # patient_global_impression_of_change_since_last_time_point
    'cortical_basal_ganglia_functional_scale': 175965,
    'psp_staging': 133558,
    'baseline_data': 184473
}

# TODOs
# table column mapping
# email regardless of error/warning

# %%
# Next, we fetch the metadata table. This table is the master table
# that contains columns and their information. It can be used to infer
# information about the columns: example, what choices are available for a
# particular question.

def _correct_response_array(metadata_df):
    '''Correct response array values for field_name
    
    Response array of the form "1, English | 2, Spanish"
    can have errors, such as missing coding, or we might
    need to change the array values - for example change
    1 to 99, or correct an array of the form "5, Never | 2.5,"
    
    This function corrects for such cases.
    
    In case a coding value is missing like for "2.5," the
    error encountered is
    File "neurobooth-terra/neurobooth_terra/redcap.py", line 183, in get_response_array
    k, v = c.strip().split(', ', maxsplit=1)
    ValueError: not enough values to unpack (expected 2, got 1) 
    
    '''
    correction_dict={}
    # Correction dictionary should have the field_name that needs correcting
    # as the key, and the actual correction as the value.
    # eg.: correction_dict['name-of-field'] = ['response-array-string','correction']
    # The value is an array of the string that needs replacing, and the string
    # that is the correction.
    # The string that needs to be corrected is generally a response array number
    # that either doesn't have a value, or has an incorrect value.
    correction_dict['prom_ataxia_54'] = ['2.5,', '2.5, Sometimes+Often']
    correction_dict['prom_ataxia_55'] = ['2.5,', '2.5, Sometimes+Often']
    correction_dict['prom_ataxia_56'] = ['2.5,', '2.5, Sometimes+Often']
    correction_dict['prom_ataxia_57'] = ['2.5,', '2.5, Sometimes+Often']
    correction_dict['prom_ataxia_58'] = ['2.5,', '2.5, Sometimes+Often']
    correction_dict['prom_ataxia_59'] = ['2.5,', '2.5, Sometimes+Often']
    correction_dict['prom_ataxia_60'] = ['2.5,', '2.5, Sometimes+Often']
    correction_dict['prom_ataxia_61'] = ['2.5,', '2.5, Sometimes+Often']
    correction_dict['prom_ataxia_62'] = ['2.5,', '2.5, Sometimes+Often']
    correction_dict['prom_ataxia_63'] = ['2.5,', '2.5, Sometimes+Often']

    for ky, vl in correction_dict.items():
        metadata_df.loc[ky, 'select_choices_or_calculations'] = metadata_df.loc[ky, 'select_choices_or_calculations'].replace(vl[0], vl[1])

    return metadata_df

print('Fetching metadata ...')
metadata = project.export_metadata(format='df')
# make any correction to redcap data dictionary as soon as it is received from redcap
metadata = _correct_response_array(metadata)
metadata_fields = ['field_label', 'form_name', 'section_header',
                   'field_type', 'select_choices_or_calculations',
                   'required_field', 'matrix_group_name', 'field_annotation',
                   'text_validation_type_or_show_slider_number']
metadata = metadata[metadata_fields]
metadata.to_csv('data_dictionary.csv')
# print(metadata.loc['prom_ataxia_54', 'select_choices_or_calculations'])
print('[Done]')

# metadata = metadata[metadata.redcap_form_name.isin(
#    ['subject', 'participant_and_consent_information', 'demograph'])]

for column in ['section_header', 'field_label']:
    metadata[column] = metadata[column].apply(
        lambda x: x.strip('\n') if isinstance(x, str) else x
    )

# feature of interest
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

# copy first section header of matrix into rest and concatenate with
# question
metadata_groups = metadata.groupby(by='matrix_group_name')
metadata['section_header'] = metadata_groups['section_header'].transform(
    lambda s: s.infer_objects().ffill())
is_group = ~pd.isna(metadata['section_header'])
metadata.loc[is_group, 'question'] = (metadata['section_header'][is_group] +
                                  metadata['question'][is_group])

metadata.to_csv('data_dictionary_modified.csv')

table_infos = get_tables_structure(metadata, include_surveys=survey_ids.keys())

metadata = metadata.reset_index()
rows_metadata, cols_metadata = dataframe_to_tuple(
    metadata, df_columns=['field_name', 'redcap_form_name',
                          'database_table_name', 'redcap_form_description',
                          'feature_of_interest', 'question', 'response_array']
)

with OptionalSSHTunnelForwarder(**ssh_args) as tunnel:
    with psycopg2.connect(**db_args) as conn:

        # Drop views first, as they may depend on the below tables
        drop_views(conn, verbose=True)

        table_metadata = Table('rc_data_dictionary', conn)
        table_metadata.insert_rows(rows_metadata, cols_metadata,
                                   on_conflict='update')

        # for table_id, table_info in table_infos.items():
        for table_id, _ in survey_ids.items():
            table_info = table_infos[table_id]
            print(f'Overwriting table {table_id}')
            drop_table('rc_' + table_id, conn)
            table = create_table('rc_' + table_id, conn, table_info['columns']+table_info['indicator_columns'],
                                 table_info['dtypes']+(['smallint[]']*len(table_info['indicator_columns'])),
                                 primary_key=['subject_id', 'redcap_event_name'])
            df = fetch_survey(project, survey_name=table_id,
                              survey_id=survey_ids[table_id])
            df = df.rename(columns={'record_id': 'subject_id'})

            # XXX: not consistent.
            complete_col = [col for col in df.columns if
                            col.endswith('complete')]
            if len(complete_col) == 0:
                warn(f'Skipping {table_id} because of missing complete col')
                continue
            df = df[df[complete_col[0]] == 2]

            report_cols = set([col.split('___')[0] for col in df.columns])
            extra_cols = report_cols - (set(table_info['columns']) |
                                        set(table_info['indicator_columns']))
            if len(extra_cols) > 0:
                raise ValueError(f'Report {table_id} contains ({extra_cols})'
                                 f'that are not found in data dictionary')

            table_info = subselect_table_structure(table_info, df.columns)
            if table_id=='prom_ataxia':
                for col in [c for c in df.columns if c.startswith('prom_ataxia_')]:
                    df[col] = [23 if v==2.5 else v for v in df[col]]
                print(df.to_csv('prom_ataxia.csv'))
            df = df.astype(dict(zip(table_info['columns'],
                                    table_info['python_dtypes'])))
            rows, columns = dataframe_to_tuple(
                df, df_columns=table_info['columns'],
                indicator_columns=table_info['indicator_columns'])

            table.insert_rows(rows, columns)

        create_views(conn, verbose=True)
