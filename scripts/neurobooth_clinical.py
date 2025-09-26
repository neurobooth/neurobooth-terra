import os
import math
import pandas as pd
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import numpy as np

import neurobooth_terra
from neurobooth_terra import list_tables, Table

import psycopg2
from sshtunnel import SSHTunnelForwarder
from config import ssh_args, db_args

def get_closest_clinical_info(df_participant, start_time, date_format, date_column_name):
    # TODO: handle missing visit_date, meaning visit_date = NULL
    if len(df_participant):
        # try:
        if isinstance(df_participant, pd.Series):
            df_participant = df_participant.to_frame().T
        
        datetime_object = datetime.strptime(start_time, date_format)
        visit_date_object = pd.to_datetime(df_participant[date_column_name], format = date_format)
        df_participant['date_diff'] = abs(visit_date_object - datetime_object)
        # closest_row = df_participant.loc[df_participant['date_diff'].idxmin()]
        closest_row = df_participant[df_participant['date_diff']==df_participant['date_diff'].min()]
        return closest_row
        # except:
            # return []
    else:
        return []

def add_clinical_flag(closest_clinical_info, closest_clinical_info_name, dict_clinical_info):
    if len(closest_clinical_info):
        dict_clinical_info[closest_clinical_info_name] = 1
        dict_clinical_info[closest_clinical_info_name + '_time_diff_between_session_and_score'] = closest_clinical_info['date_diff']
    else:
        dict_clinical_info[closest_clinical_info_name] = 0

def get_clinical_information(primary_diagnosis, subject_ID, date):
    # Using database query
    df_demographic = extract_rc_demographic_clean()
    df_current_demographic = df_demographic.loc[str(subject_ID)]
    df_current_demographic = df_current_demographic[df_current_demographic['redcap_sequence_num'] == 1]

    # Using database query
    df_ataxia_pd_scales_clean = extract_rc_ataxia_pd_scales_clean()
    df_current_ataxia_pd_scales_clean = df_ataxia_pd_scales_clean.loc[str(subject_ID)]

    date_format = "%Y-%m-%d"
    closest_ataxia_pd_scales_clean = get_closest_clinical_info(df_current_ataxia_pd_scales_clean, date, date_format, 'ataxia_pd_date_of_visit')

    # Using database query
    table_name = 'rc_prom_ataxia'
    df_prom_ataxia = get_table_from_database(table_name)
    df_current_prom_ataxia = df_prom_ataxia.loc[str(subject_ID)]

    date_format = "%Y-%m-%d"
    closest_prom_ataxia = get_closest_clinical_info(df_current_prom_ataxia, date, date_format, 'end_time_prom_ataxia')

    # Using database query
    table_name = 'rc_dysarthria_impact_scale'
    df_dysarthria_impact_scale = get_table_from_database(table_name)
    df_current_dysarthria_impact_scale = df_dysarthria_impact_scale.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_dysarthria_impact_scale = get_closest_clinical_info(df_current_dysarthria_impact_scale, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_communicative_participation_item_bank'
    df_communicative_participation_item_bank = get_table_from_database(table_name)
    df_current_communicative_participation_item_bank = df_communicative_participation_item_bank.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_communicative_participation_item_bank = get_closest_clinical_info(df_current_communicative_participation_item_bank, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_neuro_qol_anxiety_short_form'
    df_neuro_qol_anxiety_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_anxiety_short_form = df_neuro_qol_anxiety_short_form.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_neuro_qol_anxiety_short_form = get_closest_clinical_info(df_current_neuro_qol_anxiety_short_form, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_neuro_qol_cognitive_function_short_form'
    df_neuro_qol_cognitive_function_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_cognitive_function_short_form = df_neuro_qol_cognitive_function_short_form.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_neuro_qol_cognitive_function_short_form = get_closest_clinical_info(df_current_neuro_qol_cognitive_function_short_form, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_neuro_qol_depression_short_form'
    df_neuro_qol_depression_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_depression_short_form = df_neuro_qol_depression_short_form.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_neuro_qol_depression_short_form = get_closest_clinical_info(df_current_neuro_qol_depression_short_form, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_neuro_qol_emotional_dyscontrol_short_form'
    df_neuro_qol_emotional_dyscontrol_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_emotional_dyscontrol_short_form = df_neuro_qol_emotional_dyscontrol_short_form.loc[str(subject_ID)]
    closest_neuro_qol_emotional_dyscontrol_short_form = get_closest_clinical_info(df_current_neuro_qol_emotional_dyscontrol_short_form, date, date_format, 'end_time_' + '_'.join(table_name.split('_')[1:]))

    # Using database query
    table_name = 'rc_neuro_qol_fatigue_short_form'
    df_neuro_qol_fatigue_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_fatigue_short_form = df_neuro_qol_fatigue_short_form.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_neuro_qol_fatigue_short_form = get_closest_clinical_info(df_current_neuro_qol_fatigue_short_form, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_neuro_qol_le_short_form'
    df_neuro_qol_le_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_le_short_form = df_neuro_qol_le_short_form.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_neuro_qol_le_short_form = get_closest_clinical_info(df_current_neuro_qol_le_short_form, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_neuro_qol_participate_social_roles_short_form'
    df_neuro_qol_participate_social_roles_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_participate_social_roles_short_form = df_neuro_qol_participate_social_roles_short_form.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_neuro_qol_participate_social_roles_short_form = get_closest_clinical_info(df_current_neuro_qol_participate_social_roles_short_form, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_neuro_qol_positive_affect_and_wellbeing_short_form'
    df_neuro_qol_positive_affect_and_wellbeing_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_positive_affect_and_wellbeing_short_form = df_neuro_qol_positive_affect_and_wellbeing_short_form.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_neuro_qol_positive_affect_and_wellbeing_short_form = get_closest_clinical_info(df_current_neuro_qol_positive_affect_and_wellbeing_short_form, date, date_format, completion_time_name)

    # # Using database query
    # This table is empty because this short form was not administered
    # table_name = 'rc_neuro_qol_satisfaction_social_roles_short_form'
    # df_neuro_qol_satisfaction_social_roles_short_form = get_table_from_database(table_name)
    # df_current_neuro_qol_satisfaction_social_roles_short_form = df_neuro_qol_satisfaction_social_roles_short_form.loc[str(subject_ID)]
    # completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    # closest_neuro_qol_satisfaction_social_roles_short_form = get_closest_clinical_info(df_current_neuro_qol_satisfaction_social_roles_short_form, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_neuro_qol_sleep_disturbance_short_form'
    df_neuro_qol_sleep_disturbance_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_sleep_disturbance_short_form = df_neuro_qol_sleep_disturbance_short_form.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_neuro_qol_sleep_disturbance_short_form = get_closest_clinical_info(df_current_neuro_qol_sleep_disturbance_short_form, date, date_format, completion_time_name)

    # # Using database query
    # table_name = 'rc_neuro_qol_stigma_short_form'
    # This table is empty because this short form was not administered
    # df_neuro_qol_stigma_short_form = get_table_from_database(table_name)
    # df_current_neuro_qol_stigma_short_form = df_neuro_qol_stigma_short_form.loc[str(subject_ID)]
    # completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    # closest_neuro_qol_stigma_short_form = get_closest_clinical_info(df_current_neuro_qol_stigma_short_form, date, date_format, completion_time_name)

    # Using database query
    table_name = 'rc_neuro_qol_ue_short_form'
    df_neuro_qol_ue_short_form = get_table_from_database(table_name)
    df_current_neuro_qol_ue_short_form = df_neuro_qol_ue_short_form.loc[str(subject_ID)]
    completion_time_name = 'end_time_' + '_'.join(table_name.split('_')[1:])
    closest_neuro_qol_ue_short_form = get_closest_clinical_info(df_current_neuro_qol_ue_short_form, date, date_format, completion_time_name)

    dict_clinical_info = {}
    dict_clinical_info['age_first_contact'] = df_current_demographic.iloc[0]['age_first_contact']
    dict_clinical_info['gender'] = df_current_demographic.iloc[0]['gender']

    # use separate for loops to ensure that the first columns will be the boolean flags whether the scales exist or not
    add_clinical_flag(closest_ataxia_pd_scales_clean, 'ataxia_pd_scales', dict_clinical_info)
    add_clinical_flag(closest_prom_ataxia, 'prom_ataxia', dict_clinical_info)
    add_clinical_flag(closest_dysarthria_impact_scale, 'dysarthria_impact_scale', dict_clinical_info)
    add_clinical_flag(closest_communicative_participation_item_bank, 'communicative_participation_item_bank', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_anxiety_short_form, 'neuro_qol_anxiety_short_form', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_cognitive_function_short_form, 'neuro_qol_cognitive_function_short_form', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_depression_short_form, 'neuro_qol_depression_short_form', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_emotional_dyscontrol_short_form, 'neuro_qol_emotional_dyscontrol_short_form', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_fatigue_short_form, 'neuro_qol_fatigue_short_form', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_le_short_form, 'neuro_qol_le_short_form', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_participate_social_roles_short_form, 'neuro_qol_participate_social_roles_short_form', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_positive_affect_and_wellbeing_short_form, 'neuro_qol_positive_affect_and_wellbeing_short_form', dict_clinical_info)
    # add_clinical_flag(closest_neuro_qol_satisfaction_social_roles_short_form, 'neuro_qol_satisfaction_social_roles_short_form', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_sleep_disturbance_short_form, 'neuro_qol_sleep_disturbance_short_form', dict_clinical_info)
    # add_clinical_flag(closest_neuro_qol_stigma_short_form, 'neuro_qol_stigma_short_form', dict_clinical_info)
    add_clinical_flag(closest_neuro_qol_ue_short_form, 'neuro_qol_ue_short_form', dict_clinical_info)
    
    if len(closest_ataxia_pd_scales_clean):        
        dict_clinical_info.update(closest_ataxia_pd_scales_clean.to_dict())
    
    if len(closest_prom_ataxia):        
        dict_clinical_info.update(closest_prom_ataxia.to_dict())

    if len(closest_dysarthria_impact_scale):        
        dict_clinical_info.update(closest_dysarthria_impact_scale.to_dict())

    pd_clinical_info = pd.DataFrame.from_dict(dict_clinical_info)
    # pd_clinical_info.to_csv('test_clinical.csv')

    return pd_clinical_info

def get_all_clinical_information(primary_diagnosis, list_subject_ID_date):
    # TODO: not efficient since it calls the get_table_from_database everytime
    list_dict_clinical_info = []

    for subject_ID_date in list_subject_ID_date:
        subject_ID, date = subject_ID_date.split('_')
        subject_ID = int(subject_ID)
        dict_clinical_info = get_clinical_information(primary_diagnosis, subject_ID, date)
        list_dict_clinical_info.append(dict_clinical_info)

    df_clinical_info = pd.DataFrame.from_records(list_dict_clinical_info)

    return df_clinical_info

def calculate_age(df, patient_id, session_date, date_format = "%Y-%m-%d"):
    try:
        df_current_demographic = df.loc[str(patient_id)]
        if isinstance(df_current_demographic, pd.DataFrame):
            df_relative_delta = relativedelta(datetime.strptime(session_date, date_format).date(), df_current_demographic.iloc[0]['date_of_birth'])
        elif isinstance(df_current_demographic, pd.Series):
            df_relative_delta = relativedelta(datetime.strptime(session_date, date_format).date(), df_current_demographic['date_of_birth'])
        return round(df_relative_delta.years + df_relative_delta.months/12 + df_relative_delta.days/365)
    except Exception as e:
        print(patient_id)
        print(e)
        return 0

def populate_clinical_information(patient_ID, date, df_demographic, df_clinical, dict_clinical_info_tables, list_clinical_table_names, date_format, all_missing_clinical_info):
    dict_clinical_info = {}

    try:
        df_current_demographic = df_demographic.loc[str(patient_ID)]
    except:
        current_missing_clinical_info = {'patient_id': patient_ID, 'table_name': 'demographic'}
        all_missing_clinical_info.append(current_missing_clinical_info)
        print('MISSING', patient_ID, 'demographic')
        return {'patient_id': patient_ID, 'session_date': date}

    if isinstance(df_current_demographic, pd.DataFrame):
        df_current_demographic = df_current_demographic[df_current_demographic['redcap_sequence_num'] == 1]
    elif isinstance(df_current_demographic, pd.Series):
        df_current_demographic = df_current_demographic.to_frame().T

    try:
        df_current_clinical = df_clinical.loc[str(patient_ID)]
    except:
        current_missing_clinical_info = {'patient_id': patient_ID, 'table_name': 'clinical'}
        all_missing_clinical_info.append(current_missing_clinical_info)
        return None

    if isinstance(df_current_clinical, pd.DataFrame):
        df_current_clinical = get_closest_clinical_info(df_current_clinical, date, date_format, 'end_time_clinical')
        # df_current_clinical = df_current_clinical[df_current_clinical['redcap_sequence_num'] == '1']
    elif isinstance(df_current_clinical, pd.Series):
        # df_current_clinical = pd.DataFrame(df_current_clinical)
        df_current_clinical = df_current_clinical.to_frame().T

    dict_clinical_info['patient_id'] = patient_ID
    dict_clinical_info['session_date'] = date
    df_relative_delta = relativedelta(datetime.strptime(date, date_format).date(), df_current_demographic.iloc[0]['date_of_birth'])
    # dict_clinical_info['date_of_birth'] = df_current_demographic.iloc[0]['date_of_birth']
    # dict_clinical_info['age_at_NB_session'] = (datetime.strptime(date, date_format).date() - date_time_string).days // 365.2425
    dict_clinical_info['age_at_NB_session'] = round(df_relative_delta.years + df_relative_delta.months/12 + df_relative_delta.days/365)

    current_age_first_contact = df_current_demographic.iloc[0]['age_first_contact']

    if not math.isnan(current_age_first_contact):
        dict_clinical_info['age_first_contact'] = round(float(current_age_first_contact))
    else:
        dict_clinical_info['age_first_contact'] = None

    dict_clinical_info['gender'] = df_current_demographic.iloc[0]['gender']
    dict_clinical_info['primary_language'] = df_current_demographic.iloc[0]['primary_language']
    dict_clinical_info['first_language'] = df_current_demographic.iloc[0]['first_language']

    current_age_symptom_onset = df_current_clinical.iloc[0]['age_symptom_onset']
    if current_age_symptom_onset:
        dict_clinical_info['age_symptom_onset'] = round(float(current_age_symptom_onset))
    else:
        dict_clinical_info['age_symptom_onset'] = None

    current_year_primary_diagnosis = df_current_clinical.iloc[0]['year_primary_diagnosis']

    if current_year_primary_diagnosis:
        df_relative_delta = relativedelta(current_year_primary_diagnosis, df_current_demographic.iloc[0]['date_of_birth'])
        dict_clinical_info['age_primary_diagnosis'] = round(df_relative_delta.years + df_relative_delta.months/12 + df_relative_delta.days/365)
    else:
        dict_clinical_info['age_primary_diagnosis'] = None

    # dict_clinical_info['year_primary_diagnosis'] = df_current_clinical.iloc[0]['year_primary_diagnosis']

    # dict_clinical_info['age_primary_diagnosis'] = df_current_clinical.iloc[0]['year_primary_diagnosis']
    # dict_clinical_info['primary_diagnosis_at_visit'] = pd.Series(list(df_clinical.iloc[0]['primary_diagnosis_at_visit']), index=[str(patient_ID)])
    # dict_clinical_info['latest_primary_diagnosis'] = pd.Series(list(df_clinical.iloc[0]['latest_primary_diagnosis']), index=[str(patient_ID)])

    dict_clinical_info['primary_diagnosis_at_visit'] = pd.Series(list(df_current_clinical.iloc[0]['primary_diagnosis_at_visit'])).iloc[0]
    dict_clinical_info['latest_primary_diagnosis'] = pd.Series(list(df_current_clinical.iloc[0]['latest_primary_diagnosis'])).iloc[0]

    for clinical_table_name in list_clinical_table_names:
        try:
            df_current_clinical_info = dict_clinical_info_tables[clinical_table_name].loc[str(patient_ID)]
        except:
            current_missing_clinical_info = {'patient_id': patient_ID, 'table_name': clinical_table_name}
            all_missing_clinical_info.append(current_missing_clinical_info)
            continue

        
        if clinical_table_name == 'rc_ataxia_pd_scales':
            completion_time_name = 'visit_date_ataxia_pd_scales'
            closest_clinical_info = get_closest_clinical_info(df_current_clinical_info, date, date_format, completion_time_name)
            if closest_clinical_info.empty:
                completion_time_name = 'end_time_ataxia_pd_scales'
                closest_clinical_info = get_closest_clinical_info(df_current_clinical_info, date, date_format, completion_time_name)
        elif clinical_table_name == 'rc_alsfrs':
            completion_time_name = 'visit_date_alsfrs'
            closest_clinical_info = get_closest_clinical_info(df_current_clinical_info, date, date_format, completion_time_name)
        else:
            completion_time_name = 'end_time_' + '_'.join(clinical_table_name.split('_')[1:])
            closest_clinical_info = get_closest_clinical_info(df_current_clinical_info, date, date_format, completion_time_name)
            del closest_clinical_info[clinical_table_name[3:] + '_complete']
            del closest_clinical_info['start_time_' + clinical_table_name[3:]]
            del closest_clinical_info['end_time_' + clinical_table_name[3:]]
            del closest_clinical_info['comments_' + clinical_table_name[3:]]
            del closest_clinical_info['redcap_repeat_instance']
            del closest_clinical_info['redcap_repeat_instrument']
            del closest_clinical_info['redcap_event_name']
            caregiver_or_participant_complete_name = 'caregiver_or_participant_completed_' + clinical_table_name[3:]
            del closest_clinical_info[caregiver_or_participant_complete_name[:63]]
        
        dict_clinical_info['time_diff_between_session_and_score_' + clinical_table_name] = closest_clinical_info['date_diff'].iloc[0]
        # add_clinical_flag(closest_clinical_info, clinical_table_name, dict_clinical_info)
        del closest_clinical_info['date_diff']

        dict_clinical_info.update(closest_clinical_info.iloc[0].to_dict())

    return dict_clinical_info

def get_specific_clinical_information(df_demographic, df_clinical, df_ataxia_pd_scales_clean, dict_clinical_info_tables, list_clinical_table_names, list_patient_IDs, list_session_dates, date_format):
    report_clinical_info = []
    all_missing_clinical_info = []

    for patient_ID, date in zip(list_patient_IDs, list_session_dates):
        # if patient_ID != 100677:
        #     continue
        dict_clinical_info = populate_clinical_information(patient_ID, date, df_demographic, df_clinical, dict_clinical_info_tables, list_clinical_table_names, date_format, all_missing_clinical_info)
        if dict_clinical_info:
            report_clinical_info.append(dict_clinical_info)

    return report_clinical_info, all_missing_clinical_info

def filter_clinical_information(df_clinical, clinical_info_to_filter, logical_operation, clinical_info_val):
    df_filtered_clinical = []
    indices = []
    for i, row in df_clinical.iterrows():
        if logical_operation == '==':
            if row[clinical_info_to_filter] == clinical_info_val:
                df_filtered_clinical.append(row)
                indices.append(i)

    df_filtered_clinical = pd.DataFrame(df_filtered_clinical)

    return df_filtered_clinical, indices

def convert_boolean(x):
    if x == 1:
        return True
    elif np.isnan(x):
        return np.nan
    
def is_assessment_performed(x, assessment):
    if assessment in x:
        return True
    else:
        return False
    
def convert_to_half_points(x):
    if x>95:
        return np.nan
    else:
        return x/10
    
def convert_to_clean(x):
    if x>95:
        return np.nan
    else:
        return x

def convert_rc_ataxia_pd_scales(df_rc_ataxia_pd_scales):
    # print(df_rc_ataxia_pd_scales)
    # print(df_rc_ataxia_pd_scales.iloc[26]['in_person_boolean_ataxia_pd_scales'])

    df_rc_ataxia_pd_scales_clean = pd.DataFrame()
    df_rc_ataxia_pd_scales_clean.index = df_rc_ataxia_pd_scales.index
    df_rc_ataxia_pd_scales_clean['redcap_event_name'] = df_rc_ataxia_pd_scales['redcap_event_name']
    df_rc_ataxia_pd_scales_clean['redcap_sequence_num'] = df_rc_ataxia_pd_scales['redcap_event_name'].apply(convert_redcap_sequence_num)
    df_rc_ataxia_pd_scales_clean['redcap_study_arm'] = df_rc_ataxia_pd_scales['redcap_event_name'].apply(lambda x: x[-1])
    df_rc_ataxia_pd_scales_clean['ataxia_pd_scales_complete'] = df_rc_ataxia_pd_scales['ataxia_pd_scales_complete'].apply(lambda x: True if x == 2 else False)
    df_rc_ataxia_pd_scales_clean['visit_date_ataxia_pd_scales'] = df_rc_ataxia_pd_scales['visit_date_ataxia_pd_scales']
    df_rc_ataxia_pd_scales_clean['ataxia_pd_date_of_visit'] = df_rc_ataxia_pd_scales['ataxia_pd_date_of_visit']
    df_rc_ataxia_pd_scales_clean['end_time_ataxia_pd_scales'] = df_rc_ataxia_pd_scales['end_time_ataxia_pd_scales']
    df_rc_ataxia_pd_scales_clean['in_person_boolean'] = df_rc_ataxia_pd_scales['in_person_boolean_ataxia_pd_scales'].apply(convert_boolean)
    
    # TODO: df_rc_ataxia_pd_scales_clean['rater_name']

    df_rc_ataxia_pd_scales_clean['comments_ataxia_pd_scales'] = df_rc_ataxia_pd_scales['comments_ataxia_pd_scales']
    df_rc_ataxia_pd_scales_clean['bars_performed_boolean'] = df_rc_ataxia_pd_scales['ataxia_pd_assessments_performed'].apply(is_assessment_performed, assessment=1)

    columns_bars = ['bars_gait', 'bars_heel_shin_right', 'bars_heel_shin_left', 'bars_finger_nose_right', 'bars_finger_nose_left', 'bars_speech', 'bars_oculomotor']
    columns_bars_comments = ['bars_gait_comments', 'bars_heel_shin_comments', 'bars_finger_nose_comments', 'bars_speech_comments', 'bars_oculomotor_comments']

    for column_bars in columns_bars:
        df_rc_ataxia_pd_scales_clean[column_bars] = df_rc_ataxia_pd_scales[column_bars].apply(convert_to_half_points)

    for column_bars_comments in columns_bars_comments:
        df_rc_ataxia_pd_scales_clean[column_bars_comments] = df_rc_ataxia_pd_scales[column_bars_comments]  

    df_rc_ataxia_pd_scales_clean['bars_total'] = df_rc_ataxia_pd_scales['bars_total']
    
    df_rc_ataxia_pd_scales_clean['sara_performed_boolean'] = df_rc_ataxia_pd_scales['ataxia_pd_assessments_performed'].apply(is_assessment_performed, assessment=2)

    columns_sara = ['sara_gait_1', 'sara_stance_2', 'sara_sitting_3', 'sara_speech_disturbance_4', 'sara_finger_chase_right_5', 
                    'sara_finger_chase_left_5', 'sara_finger_nose_right_6', 'sara_finger_nose_left_6', 
                    'sara_alternating_hand_movements_right_7', 'sara_alternating_hand_movements_left_7', 
                    'sara_heel_shin_right_8', 'sara_heel_shin_left_8']
    
    columns_sara_comments = ['sara_gait_1_comments', 'sara_stance_2_comments', 'sara_sitting_3_comments', 
                             'sara_speech_disturbance_4_comments', 'sara_finger_chase_5_comments', 
                             'sara_finger_nose_6_comments', 'sara_alternating_hand_movements_7_comments', 
                             'sara_heel_shin_comments']

    for column_sara in columns_sara:
        new_column_sara = '_'.join([word for word in column_sara.split('_') if not word.isdigit()])
        df_rc_ataxia_pd_scales_clean[new_column_sara] = df_rc_ataxia_pd_scales[column_sara].apply(convert_to_clean)

    for column_sara_comments in columns_sara_comments:
        new_column_sara_comments = '_'.join([word for word in column_sara_comments.split('_') if not word.isdigit()])
        df_rc_ataxia_pd_scales_clean[new_column_sara_comments] = df_rc_ataxia_pd_scales[column_sara_comments]  

    df_rc_ataxia_pd_scales_clean['sara_total'] = df_rc_ataxia_pd_scales['sara_total']

    df_rc_ataxia_pd_scales_clean['micars_performed_boolean'] = df_rc_ataxia_pd_scales['ataxia_pd_assessments_performed'].apply(is_assessment_performed, assessment=4)
    
    columns_micars = ['micars_rebound_right_16', 'micars_rebound_left_16', 'micars_overshoot_arms_right_17', 'micars_overshoot_arms_left_17',
                      'micars_spiral_18', 'micars_dysarthria_fluency_of_speech_19', 'micars_dysarthria_clarity_of_speech_20', 'micars_dysarthria_alternating_syllables_21',
                      'micars_nystagmus_23', 'micars_abnormalities_ocular_pursuit_24', 'micars_dysmetria_saccade_25', 'micars_saccadic_intrusions_26']
    
    columns_micars_comments = ['micars_arms_comments', 'micars_speech_comments', 'micars_oculomotor_comments']

    for column_micars in columns_micars:
        df_rc_ataxia_pd_scales_clean['_'.join(column_micars.split('_')[:-1])] = df_rc_ataxia_pd_scales[column_micars]

    for column_micars_comments in columns_micars_comments:
        df_rc_ataxia_pd_scales_clean[column_micars_comments] = df_rc_ataxia_pd_scales[column_micars_comments] 

    df_rc_ataxia_pd_scales_clean['updrs_performed_boolean'] = df_rc_ataxia_pd_scales['ataxia_pd_assessments_performed'].apply(is_assessment_performed, assessment=3)
    
    columns_updrs = ['updrs_speech_1', 'updrs_facial_expressions_2', 'updrs_rigidity_neck_3', 'updrs_rigidity_rue_3',
                    'updrs_rigidity_lue_3', 'updrs_rigidity_rle_3', 'updrs_rigidity_lle_3', 'updrs_finger_tapping_right_4',
                    'updrs_finger_tapping_left_4', 'updrs_hand_movements_right_5', 'updrs_hand_movements_left_5',
                    'updrs_pronation_supination_right_6', 'updrs_pronation_supination_left_6', 'updrs_toe_tapping_right_7',
                    'updrs_toe_tapping_left_7', 'updrs_leg_agility_right_8', 'updrs_leg_agility_left_8', 'updrs_arising_from_chair_9',
                    'updrs_gait_10', 'updrs_gait_freeze_11', 'updrs_postural_stability_12', 'updrs_posture_13', 'updrs_global_spontaneity_of_movement_14',
                    'updrs_postural_tremor_right_15', 'updrs_postural_tremor_left_15', 'updrs_kinetic_tremor_right_16', 'updrs_kinetic_tremor_left_16',
                    'updrs_rest_tremor_rue_17', 'updrs_rest_tremor_lue_17', 'updrs_rest_tremor_rle_17', 'updrs_rest_tremor_lle_17', 'updrs_rest_tremor_lip_jaw_17',
                    'updrs_tremor_constancy_of_rest_tremor_18', 'updrs_hoehn_yahr_stage', 'updrs_dyskinesia_boolean', 'updrs_dyskinesia_interfere']
    
    columns_updrs_comments = ['updrs_speech_1_comments', 'updrs_rigidity_comments_3', 'updrs_finger_tapping_4_comments', 'updrs_hand_movements_5_comments',
                            'updrs_pronation_supination_6_comments', 'updrs_toe_tapping_7_comments', 'updrs_leg_agility_8_comments', 'updrs_arising_from_chair_9_comments',
                            'updrs_gait_10_comments', 'updrs_gait_freeze_11_comments', 'updrs_postural_stability_12_comments', 'updrs_posture_13_comments',
                            'updrs_global_spontaneity_of_movement_14_comments', 'updrs_postural_tremor_15_comments', 'updrs_kinetic_tremor_16_comments',
                            'updrs_rest_tremor_17_comments', 'updrs_tremor_constancy_of_rest_tremor_18_comments']

    for column_updrs in columns_updrs:
        new_column_updrs_comments = '_'.join([column_updrs.split('_')[:-1] if column_updrs.split('_')[-1].isdigit() else column_updrs.split('_')][0])
        df_rc_ataxia_pd_scales_clean[new_column_updrs_comments] = df_rc_ataxia_pd_scales[column_updrs].apply(convert_to_clean)
    
    for column_updrs_comments in columns_updrs_comments:
        new_column_updrs_comments = '_'.join([word for word in column_updrs_comments.split('_') if not word.isdigit()])
        df_rc_ataxia_pd_scales_clean[new_column_updrs_comments] = df_rc_ataxia_pd_scales[column_updrs_comments] 

    df_rc_ataxia_pd_scales_clean['updrs_total'] = df_rc_ataxia_pd_scales['updrs_total']

    df_rc_ataxia_pd_scales_clean['levodopa_minutes'] = df_rc_ataxia_pd_scales['levodopa_minutes_ataxia_pd_scales']

    df_rc_ataxia_pd_scales_clean['uhdrs_performed_boolean'] = df_rc_ataxia_pd_scales['ataxia_pd_assessments_performed'].apply(is_assessment_performed, assessment=5)

    columns_uhdrs = ['uhdrs_pursuit_horizontal_1a', 'uhdrs_pursuit_vertical_1b', 'uhdrs_saccade_initation_horizontal_2a',
                    'uhdrs_oculomotor_saccade_initation_horizontal', 'uhdrs_saccade_initation_vertical_2b', 'uhdrs_oculomotor_saccade_initation_vertical',
                    'uhdrs_saccade_velocity_horizontal_3a', 'uhdrs_oculomotor_saccade_velocity_horizontal', 'uhdrs_saccade_velocity_vertical_3b',
                    'uhdrs_oculomotor_saccade_velocity_vertical', 'uhdrs_dysarthria_4', 'uhdrs_tongue_protrusion_5', 'uhdrs_finger_taps_right_6a',
                    'uhdrs_finger_taps_left_6b', 'uhdrs_pronate_supinate_hands_right_7a', 'uhdrs_pronate_supinate_hands_left_7b', 'uhdrs_luria',
                    'uhdrs_rigidity_arms_right_9a', 'uhdrs_rigidity_arms_left_9b', 'uhdrs_bradykinesia_body', 'uhdrs_maximal_dystonia_trunk_11a',
                    'uhdrs_maximal_dystonia_rue_11b', 'uhdrs_maximal_dystonia_lue_11c', 'uhdrs_maximal_dystonia_rle_11d', 'uhdrs_maximal_dystonia_lle_11e',
                    'uhdrs_maximal_chorea_face_12a', 'uhdrs_maximal_chorea_bol_12b', 'uhdrs_maximal_chorea_trunk_12c', 'uhdrs_maximal_chorea_rue_12d',
                    'uhdrs_maximal_chorea_lue_12e', 'uhdrs_maximal_chorea_rle_12f', 'uhdrs_maximal_chorea_lle_12g', 'uhdrs_gait_13', 'uhdrs_tandem_walking_14',
                    'uhdrs_tandem_retropulsion_pull_test_15', 'uhdrs_diagnosis_confidence_level_17']
                    													
    for column_uhdrs in columns_uhdrs:
        df_rc_ataxia_pd_scales_clean['_'.join([column_uhdrs.split('_')[:-1] if any([char.isdigit() for char in column_uhdrs.split('_')[-1]]) else column_uhdrs.split('_')][0])] = df_rc_ataxia_pd_scales[column_uhdrs]
    
    # df_rc_ataxia_pd_scales_clean.to_csv('test_clinical.csv')

    return df_rc_ataxia_pd_scales_clean

def double_check_latest_diagnosis(df_rc_clinical, row, primary_diag_set, key):
    x = row.name

    if df_rc_clinical is None:
        primary_diag_set.add(row[key])
    else:
        current_rc_clinical = df_rc_clinical.loc[str(x)]
        if isinstance(current_rc_clinical, pd.Series):
            primary_diag_set.add(row[key])
        elif isinstance(current_rc_clinical, pd.DataFrame):
            primary_diag_set.add(current_rc_clinical.sort_values(by=['date_enrolled'], ascending=False).iloc[0][key])

def convert_diagnosis(row, df_rc_data_dictionary, key, df_rc_clinical=None):
    primary_diag_set = set()

    for diagnosis in row[key]:
        if diagnosis == 5:
            double_check_latest_diagnosis(df_rc_clinical, row, primary_diag_set, 'other_dementia')
            # primary_diag_set.add(row['other_dementia'])
        elif diagnosis == 13:
            double_check_latest_diagnosis(df_rc_clinical, row, primary_diag_set, 'other_neuropathy')
            # primary_diag_set.add(row['other_neuropathy'])
        elif diagnosis == 23:
            double_check_latest_diagnosis(df_rc_clinical, row, primary_diag_set, 'other_ataxia')
        elif diagnosis == 24:
            double_check_latest_diagnosis(df_rc_clinical, row, primary_diag_set, 'other_primary_diagnosis')
            # primary_diag_set.add(row['other_primary_diagnosis'])
        else:
            primary_diag_set.add(df_rc_data_dictionary.loc['primary_diagnosis']['response_array'][str(int(diagnosis))])

    return primary_diag_set

def check_latest_primary_diagnosis_id(x, df_rc_clinical):
    current_rc_clinical = df_rc_clinical.loc[str(x)]
    if isinstance(current_rc_clinical, pd.Series):
        return set(current_rc_clinical['primary_diagnosis'])
    elif isinstance(current_rc_clinical, pd.DataFrame):
        # print(current_rc_clinical.sort_values(by=['date_enrolled'], ascending=False).iloc[0]['primary_diagnosis'])
        return set(current_rc_clinical.sort_values(by=['date_enrolled'], ascending=False).iloc[0]['primary_diagnosis'])

def convert_past_or_present(x, flag):
    if flag in x:
        return True
    else:
        return False

def convert_redcap_sequence_num(x):
    sequence_num = x.split('_')[0]
    if sequence_num == 'enrollment':
        return 1
    else:
        return sequence_num[-1]

def get_id_from_baseline(x, df_rc_baseline_data, unique_id):
    try:
        return df_rc_baseline_data.loc[str(x)][unique_id]
    except:
        return ''

def convert_rc_clinical(df_rc_clinical, df_rc_baseline_data, df_rc_data_dictionary):
    df_rc_clinical_clean = pd.DataFrame()
    df_rc_clinical_clean.index = df_rc_clinical.index
    df_rc_clinical_clean['redcap_event_name'] = df_rc_clinical['redcap_event_name']
    df_rc_clinical_clean['redcap_sequence_num'] = df_rc_clinical['redcap_event_name'].apply(convert_redcap_sequence_num)
    df_rc_clinical_clean['redcap_study_arm'] = df_rc_clinical['redcap_event_name'].apply(lambda x: x[-1])
    df_rc_clinical_clean['clinical_complete'] = df_rc_clinical['clinical_complete'].apply(lambda x: True if x == 2 else False)
    df_rc_clinical_clean['date_enrolled'] = df_rc_clinical['date_enrolled']
    df_rc_clinical_clean['start_time_clinical'] = df_rc_clinical['start_time_clinical']
    df_rc_clinical_clean['end_time_clinical'] = df_rc_clinical['end_time_clinical']
    # df_rc_clinical_clean['neurologist'] =
    df_rc_clinical_clean['height'] = df_rc_clinical['height']
    df_rc_clinical_clean['weight'] = df_rc_clinical['weight']
    df_rc_clinical_clean['primary_diagnosis_at_visit'] = df_rc_clinical.apply(lambda row: convert_diagnosis(row, df_rc_data_dictionary=df_rc_data_dictionary, key='primary_diagnosis'), axis=1)
    df_rc_clinical_clean['primary_diagnosis_id_at_visit'] = df_rc_clinical['primary_diagnosis']
    df_rc_clinical['latest_primary_diagnosis_id'] = df_rc_clinical_clean.index.to_series().apply(check_latest_primary_diagnosis_id, df_rc_clinical=df_rc_clinical)
    df_rc_clinical_clean['latest_primary_diagnosis'] = df_rc_clinical.apply(lambda row: convert_diagnosis(row, df_rc_data_dictionary=df_rc_data_dictionary, key='latest_primary_diagnosis_id', df_rc_clinical=df_rc_clinical), axis=1)
    df_rc_clinical_clean['latest_primary_diagnosis_id'] = df_rc_clinical['latest_primary_diagnosis_id']

    df_rc_clinical_clean['year_primary_diagnosis'] = df_rc_clinical['year_primary_diagnosis']
    df_rc_clinical_clean['secondary_diagnosis'] = df_rc_clinical['secondary_diagnosis']
    df_rc_clinical_clean['diagnosis_notes'] = df_rc_clinical['diagnosis_notes']
    df_rc_clinical_clean['year_secondary_diagnosis'] = df_rc_clinical['year_secondary_diagnosis']
    df_rc_clinical_clean['age_symptom_onset'] = df_rc_clinical['age_symptom_onset']
    df_rc_clinical_clean['first_symptom'] = df_rc_clinical['first_symptom']

    df_rc_clinical_clean['genetic_testing_boolean'] = df_rc_clinical['genetic_testing_boolean']
    df_rc_clinical_clean['mri_boolean'] = df_rc_clinical['mri_boolean']
    df_rc_clinical_clean['additional_medical_conditions'] = df_rc_clinical['additional_medical_conditions']
    df_rc_clinical_clean['musculoskeletal_conditions'] = df_rc_clinical['musculoskeletal_conditions']
    df_rc_clinical_clean['limb_movement_conditions'] = df_rc_clinical['limb_movement_conditions']
    df_rc_clinical_clean['walking_conditions'] = df_rc_clinical['walking_conditions']
    df_rc_clinical_clean['balance_conditions'] = df_rc_clinical['balance_conditions']
    df_rc_clinical_clean['speech_conditions'] = df_rc_clinical['speech_conditions']
    df_rc_clinical_clean['vision_conditions'] = df_rc_clinical['vision_conditions']
    df_rc_clinical_clean['hearing_conditions'] = df_rc_clinical['hearing_conditions']
    df_rc_clinical_clean['assistive_walking_device_boolean'] = df_rc_clinical['assistive_walking_device_boolean']
    df_rc_clinical_clean['assistive_walking_device_type'] = df_rc_clinical['assistive_walking_device_type']
    df_rc_clinical_clean['assistive_walking_device_percent_time_usage'] = df_rc_clinical['assistive_walking_device_percent_time_usage']

    df_rc_clinical_clean['stroke_boolean'] = df_rc_clinical['stroke_boolean']
    df_rc_clinical_clean['neuropathy_boolean'] = df_rc_clinical['neuropathy_boolean']
    df_rc_clinical_clean['dyslexia_boolean'] = df_rc_clinical['dyslexia_boolean']

    df_rc_clinical_clean['current_medications'] = df_rc_clinical['dyslexia_boolean']
    df_rc_clinical_clean['previous_medications'] = df_rc_clinical['previous_medications']

    df_rc_clinical_clean['neuropheno_id'] = df_rc_clinical.index.to_series().apply(get_id_from_baseline, df_rc_baseline_data=df_rc_baseline_data, unique_id='neuropheno_id')
    df_rc_clinical_clean['remote_sca_id'] = df_rc_clinical.index.to_series().apply(get_id_from_baseline, df_rc_baseline_data=df_rc_baseline_data, unique_id='adult_remote_id')
    df_rc_clinical_clean['harvard_biomarker_boolean'] = df_rc_clinical['harvard_biomarker_boolean']
    df_rc_clinical_clean['biobank_boolean'] = df_rc_clinical['biobank_boolean']

    df_rc_clinical_clean['drug_trial_present'] = df_rc_clinical['drug_trial_past_or_present'].apply(convert_past_or_present, flag=1)
    df_rc_clinical_clean['drug_trial_past'] = df_rc_clinical['drug_trial_past_or_present'].apply(convert_past_or_present, flag=2)

    df_rc_clinical_clean['drug_trial_name'] = df_rc_clinical['drug_trial_name']

    return df_rc_clinical_clean

def check_test_subject(x, df_rc_baseline_data):
    try:
        row = df_rc_baseline_data.loc[str(x)]
        return row['test_subject_boolean']
    except:
        print(x, 'not in rc_baseline')
        return 1

def check_age_first_contact(x, df_rc_participant_and_consent_information):
    # Note that there are instances with multiple age at first contact in the rc_participant_and_consent_information due to participants needing to be reconsented after 3 years or so.
    try:
        row = df_rc_participant_and_consent_information.loc[str(x)]
        if isinstance(row, pd.DataFrame):
            row = row[row['current_instance_participant_and_consent_information']==1].iloc[0]
        
        # print(row['subject_age_first_contact'])
        # if str(x) == 100386:
        #     sys.exit()
        return row['subject_age_first_contact']
    except:
        return np.nan
    
def check_DOB(x, df_subject):
    try:
        row = df_subject.loc[str(x)]
        return row['date_of_birth_subject']
    except:
        return np.nan
    
def check_gender_at_birth(x, df_subject, df_rc_data_dictionary):
    try:
        row = df_subject.loc[str(x)]
        gender_at_birth = row['gender_at_birth']
        return df_rc_data_dictionary.loc['gender_at_birth']['response_array'][str(int(gender_at_birth))]
    except:
        return np.nan   

def convert_ID_to_string(x, df_rc_data_dictionary, key):
    try:
        if isinstance(x, float):
            return df_rc_data_dictionary.loc[key]['response_array'][str(int(x))]
        elif isinstance(x, list):
            list_infos = []
            for info in x:
                info_text = df_rc_data_dictionary.loc[key]['response_array'][str(int(info))]
                list_infos.append(info_text)
            return list_infos
        elif isinstance(x, int):
            return df_rc_data_dictionary.loc[key]['response_array'][str(int(x))]
    except:
        return np.nan

def check_race(x, df_rc_baseline_data, df_rc_data_dictionary):
    try:
        row = df_rc_baseline_data.loc[str(x)]
        races = row['race']
        list_race = []
        for race in races:
            race_text = df_rc_data_dictionary.loc['race']['response_array'][str(int(race))]
            list_race.append(race_text)
        return list_race
    except:
        return np.nan
    
def check_info_from_baseline_data(x, df_rc_baseline_data, df_rc_data_dictionary, info_name):
    try:
        row = df_rc_baseline_data.loc[str(x)]
        infos = row[info_name]
        if isinstance(infos, list):
            list_info = []
            for info in infos:
                info_text = df_rc_data_dictionary.loc[info_name]['response_array'][str(int(info))]
                list_info.append(info_text)
            return list_info
        elif isinstance(infos, float):
            return df_rc_data_dictionary.loc[info_name]['response_array'][str(int(infos))]
    except:
        return np.nan
    
def check_ancestry(x, df_rc_data_dictionary):
    try:
        list_ancestries = []
        for ancestry in x:
            race_text = df_rc_data_dictionary.loc['ancestry']['response_array'][str(int(ancestry))]
            list_ancestries.append(race_text)
        return list_ancestries
    except:
        return np.nan
    
def convert_rc_demographic(df_rc_demographic, df_rc_baseline_data, df_rc_participant_and_consent_information, df_subject, df_rc_data_dictionary):
    # TODO: REMOVE ALL test_subject_boolean == TRUE

    df_rc_demographic_clean = pd.DataFrame()
    df_rc_demographic_clean.index = df_rc_demographic.index
    df_rc_demographic_clean['redcap_event_name'] = df_rc_demographic['redcap_event_name']
    df_rc_demographic_clean['redcap_sequence_num'] = df_rc_demographic['redcap_event_name'].apply(convert_redcap_sequence_num)
    df_rc_demographic_clean['redcap_study_arm'] = df_rc_demographic['redcap_event_name'].apply(lambda x: x[-1])
    df_rc_demographic_clean['test_subject_boolean'] = df_rc_demographic.index.to_series().apply(check_test_subject, df_rc_baseline_data=df_rc_baseline_data)
    df_rc_demographic_clean['demographic_complete'] = df_rc_demographic['demographic_complete'].apply(lambda x: True if x==2 else False)
    df_rc_demographic_clean['start_time_demographic'] = df_rc_demographic['start_time_demographic']
    df_rc_demographic_clean['end_time_demographic'] = df_rc_demographic['end_time_demographic']
    df_rc_demographic_clean['age_first_contact'] = df_rc_demographic.index.to_series().apply(check_age_first_contact, df_rc_participant_and_consent_information=df_rc_participant_and_consent_information)
    df_rc_demographic_clean['date_of_birth'] = df_rc_demographic.index.to_series().apply(check_DOB, df_subject=df_subject)
    df_rc_demographic_clean['gender'] = df_rc_demographic['gender_current'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='gender')
    df_rc_demographic_clean['gender_at_birth'] = df_rc_demographic.index.to_series().apply(check_gender_at_birth, df_subject=df_subject, df_rc_data_dictionary=df_rc_data_dictionary)
    df_rc_demographic_clean['handedness'] = df_rc_demographic['handedness'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='handedness')
    df_rc_demographic_clean['race'] = df_rc_demographic.index.to_series().apply(check_info_from_baseline_data, df_rc_baseline_data=df_rc_baseline_data, df_rc_data_dictionary=df_rc_data_dictionary, info_name='race')
    df_rc_demographic_clean['ancestry'] = df_rc_demographic['ancestry'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='ancestry')
    df_rc_demographic_clean['ethnicity'] = df_rc_demographic.index.to_series().apply(check_info_from_baseline_data, df_rc_baseline_data=df_rc_baseline_data, df_rc_data_dictionary=df_rc_data_dictionary, info_name='ethnicity')
    df_rc_demographic_clean['health_history'] = df_rc_demographic['health_history'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='health_history')
    df_rc_demographic_clean['smoker_boolean'] = df_rc_demographic['smoker_boolean']
    df_rc_demographic_clean['years_smoking'] = df_rc_demographic['years_smoking']
    df_rc_demographic_clean['packs_per_day'] = df_rc_demographic['packs_per_day']
    df_rc_demographic_clean['date_last_smoked'] = df_rc_demographic['date_last_smoked']
    df_rc_demographic_clean['smartphone_owner_boolean'] = df_rc_demographic['smartphone_owner_boolean']
    df_rc_demographic_clean['smartphone_ease_of_usage'] = df_rc_demographic['smartphone_ease_of_usage']
    df_rc_demographic_clean['internet_access_boolean'] = df_rc_demographic['internet_access_boolean']
    df_rc_demographic_clean['primary_language'] = df_rc_demographic['primary_language'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='primary_language')
    # df_rc_demographic_clean['primary_language_dialect']
    df_rc_demographic_clean['first_language'] = df_rc_demographic['first_language'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='first_language')
    # df_rc_demographic_clean['first_language_dialect']
    # df_rc_demographic_clean['additional_languages']
    df_rc_demographic_clean['zipcode'] = df_rc_demographic['zipcode']
    df_rc_demographic_clean['education'] = df_rc_demographic['education'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='education')
    df_rc_demographic_clean['employment'] = df_rc_demographic['employment'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='employment')
    df_rc_demographic_clean['current_or_recent_job_title'] = df_rc_demographic['current_or_recent_job_title']
    df_rc_demographic_clean['current_or_recent_employer'] = df_rc_demographic['current_or_recent_employer']
    df_rc_demographic_clean['marital_status'] = df_rc_demographic['marital_status'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='marital_status')
    df_rc_demographic_clean['caretaker_boolean'] = df_rc_demographic['caretaker_boolean']
    df_rc_demographic_clean['past_research_experience_boolean'] = df_rc_demographic['past_research_experience_boolean']
    df_rc_demographic_clean['recruitment_mechanism'] = df_rc_demographic['recruitment_mechanism'].apply(convert_ID_to_string, df_rc_data_dictionary=df_rc_data_dictionary, key='recruitment_mechanism')
    df_rc_demographic_clean['comments_demographic'] = df_rc_demographic['comments_demographic']

    return df_rc_demographic_clean

def convert_rc_visit_dates(df_rc_visit_dates):
    # TODO: Remove test_participants
    df_rc_visit_dates_clean = pd.DataFrame()
    df_rc_visit_dates_clean.index = df_rc_visit_dates.index
    df_rc_visit_dates_clean['redcap_event_name'] = df_rc_visit_dates['redcap_event_name']
    df_rc_visit_dates_clean['redcap_sequence_num'] = df_rc_visit_dates['redcap_event_name'].apply(convert_redcap_sequence_num)
    df_rc_visit_dates_clean['redcap_study_arm'] = df_rc_visit_dates['redcap_event_name'].apply(lambda x: x[-1])
    df_rc_visit_dates_clean['visit_dates_complete'] = df_rc_visit_dates['visit_dates_complete'].apply(lambda x: True if x==2 else False)
    df_rc_visit_dates_clean['neurobooth_visit_dates'] = df_rc_visit_dates['neurobooth_visit_dates']
    df_rc_visit_dates_clean['neurobooth_visit_time'] = df_rc_visit_dates['neurobooth_visit_time']

def compute_time_delta_in_days(date1, date2, date_format):
    if not isinstance(date1, date):
        date1 = datetime.strptime(date1, date_format)
    if not isinstance(date2, date):
        date2 = datetime.strptime(date2, date_format)

    delta = date2-date1
    return delta.days

def check_longitudinal(x, df_rc_visit_dates):
    try:
        current_longitudinal = df_rc_visit_dates.loc[str(x)]
        if isinstance(current_longitudinal, pd.Series):
            num_visits = 1
            first_visit = current_longitudinal['neurobooth_visit_dates']
            last_visit = current_longitudinal['neurobooth_visit_dates']
            total_days = 0
            # return num_visits, first_visit, last_visit, total_days
        elif isinstance(current_longitudinal, pd.DataFrame):
            current_longitudinal.sort_values(by=['neurobooth_visit_dates'], ascending=True, inplace=True)
            num_visits = len(current_longitudinal)
            first_visit = current_longitudinal['neurobooth_visit_dates'].iloc[0]
            last_visit = current_longitudinal['neurobooth_visit_dates'].iloc[-1]
            date_format = "%Y-%m-%d"
            total_days = compute_time_delta_in_days(first_visit, last_visit, date_format)
            # print(current_rc_clinical.sort_values(by=['date_enrolled'], ascending=False).iloc[0]['primary_diagnosis'])
        return [num_visits, first_visit, last_visit, total_days]
    except:
        return [np.nan, np.nan, np.nan, np.nan]

def convert_v_longitudinal(df_rc_visit_dates):
    # TODO: Remove test_participants
    df_v_longitudinal = pd.DataFrame()
    df_v_longitudinal.index = df_rc_visit_dates.index.unique()
    df_new_list = df_v_longitudinal.index.to_series().apply(check_longitudinal, df_rc_visit_dates=df_rc_visit_dates)
    df_v_longitudinal[['num_visits', 'first_visit', 'last_visit', 'total_days']] =  pd.DataFrame(df_new_list.tolist(), index=df_v_longitudinal.index) #= df_v_longitudinal.index.to_series().apply(check_longitudinal, df_rc_visit_dates=df_rc_visit_dates)

    for i,row in df_v_longitudinal[df_v_longitudinal['num_visits']>1.0].iloc[4:].iterrows():
        print(row.name, row['num_visits'], row['first_visit'], row['last_visit'], row['total_days'])

def extract_rc_demographic_clean():
    # convert rc_demographic
    table_name = 'rc_demographic'
    df_rc_demographic = get_table_from_database(table_name)
    table_name = 'rc_baseline_data'
    df_rc_baseline_data = get_table_from_database(table_name)
    table_name = 'rc_participant_and_consent_information'
    df_rc_participant_and_consent_information = get_table_from_database(table_name)
    table_name = 'subject'
    df_subject = get_table_from_database(table_name)
    table_name = 'rc_data_dictionary'
    df_rc_data_dictionary = get_table_from_database(table_name)
    df_rc_demographic_clean = convert_rc_demographic(df_rc_demographic, df_rc_baseline_data, df_rc_participant_and_consent_information, df_subject, df_rc_data_dictionary)
    return df_rc_demographic_clean

def extract_rc_ataxia_pd_scales_clean():
    # convert rc_ataxia_pd_scales
    table_name = 'rc_ataxia_pd_scales'
    df_rc_ataxia_pd_scales = get_table_from_database(table_name)
    df_rc_ataxia_pd_scales_clean = convert_rc_ataxia_pd_scales(df_rc_ataxia_pd_scales)
    return df_rc_ataxia_pd_scales_clean

def extract_rc_clinical():
    # convert rc_clinical
    table_name = 'rc_clinical'
    df_rc_clinical = get_table_from_database(table_name)
    table_name = 'rc_data_dictionary'
    df_rc_data_dictionary = get_table_from_database(table_name)
    table_name = 'rc_baseline_data'
    df_rc_baseline_data = get_table_from_database(table_name)
    df_rc_clinical_clean = convert_rc_clinical(df_rc_clinical, df_rc_baseline_data, df_rc_data_dictionary)
    return df_rc_clinical_clean

def extract_rc_visit_dates():
    # convert rc_visit_dates
    table_name = 'rc_visit_dates'
    df_rc_visit_dates = get_table_from_database(table_name)
    df_rc_visit_dates_clean = convert_rc_visit_dates(df_rc_visit_dates)  
    return df_rc_visit_dates_clean

def extract_v_longitudal_summary():
    # build v_longitudal_summary
    table_name = 'rc_visit_dates'
    df_rc_visit_dates = get_table_from_database(table_name)
    df_rc_visit_dates_clean = convert_v_longitudinal(df_rc_visit_dates)
    return df_rc_visit_dates_clean

def is_subset_current_set(current_set, subset, operation):
    assert operation in ['and', 'or'], 'Operation Not Handled'

    if operation == 'and':
        if all(e in current_set for e in subset):
            return True
    elif operation == 'or':
        if any(e in current_set for e in subset):
            return True
    else:
        return False

def get_clinical_disease_categories(config, subgroups, subgroup_operation, phenotypes, phenotype_operation):
    assert subgroup_operation in ['and', 'or'], 'Subgroup Operation Not Handled'
    assert phenotype_operation in ['and', 'or'], 'Subgroup Operation Not Handled'

    dict_clinical_disease_categories = config['Diagnosis Entry from the Database']

    clinical_disease_categories = []

    for disease, disease_labels in dict_clinical_disease_categories.items():
        current_disease_subgroups = disease_labels['subgroups']
        current_disease_phenotypes = disease_labels['phenotypes']

        if len(subgroups) > 0 and len(phenotypes) > 0:
            if is_subset_current_set(current_disease_subgroups, subgroups, subgroup_operation):
                if is_subset_current_set(current_disease_phenotypes, phenotypes, phenotype_operation):
                    clinical_disease_categories.append(disease)
        else:
            if len(subgroups) > 0:
                if is_subset_current_set(current_disease_subgroups, subgroups, subgroup_operation):
                    clinical_disease_categories.append(disease)
            if len(phenotypes) > 0:
                if is_subset_current_set(current_disease_phenotypes, phenotypes, phenotype_operation):
                    clinical_disease_categories.append(disease)

    return clinical_disease_categories

def get_short_diagnosis_name(config, group_short_diagnosis_name):
    dict_clinical_disease_categories = config['Diagnosis Entry from the Database']

    clinical_disease_categories = []

    for disease, disease_labels in dict_clinical_disease_categories.items():
        current_short_diagnosis_name = disease_labels['Short Diagnosis Name']
        if current_short_diagnosis_name in group_short_diagnosis_name:
            clinical_disease_categories.append(disease)

    return clinical_disease_categories
            
def run_get_clinical_disease_categories(neurobooth_clinical_cfg, clinical_disease_categories_cfg, group):
    group_name = neurobooth_clinical_cfg[group]['name']
    group_subgroups = neurobooth_clinical_cfg[group]['subgroups']
    group_subgroup_operation = neurobooth_clinical_cfg[group]['subgroup_operation']
    group_phenotypes = neurobooth_clinical_cfg[group]['phenotypes']
    group_phenotype_operation = neurobooth_clinical_cfg[group]['phenotype_operation']
    group_short_diagnosis_name = neurobooth_clinical_cfg[group]['short_diagnosis_name']


    if group_short_diagnosis_name:
        group_clinical_disease_categories = get_short_diagnosis_name(clinical_disease_categories_cfg, group_short_diagnosis_name)
    else:
        group_clinical_disease_categories = get_clinical_disease_categories(clinical_disease_categories_cfg, group_subgroups, group_subgroup_operation, group_phenotypes, group_phenotype_operation)

    return group_name, group_clinical_disease_categories

def run_all_get_clinical_disease_categories(neurobooth_clinical_cfg_path):
    neurobooth_clinical_cfg = get_config(neurobooth_clinical_cfg_path)
    path_clinical_disease_categories_cfg = neurobooth_clinical_cfg['path_clinical_disease_categories_cfg']
    clinical_disease_categories_cfg = get_config(path_clinical_disease_categories_cfg)

    group_1_name, group_1_clinical_disease_categories = run_get_clinical_disease_categories(neurobooth_clinical_cfg, clinical_disease_categories_cfg, 'group_1')
    group_2_name, group_2_clinical_disease_categories = run_get_clinical_disease_categories(neurobooth_clinical_cfg, clinical_disease_categories_cfg, 'group_2')

    return group_1_name, group_1_clinical_disease_categories, group_2_name, group_2_clinical_disease_categories

def extract_demographic_clinical_and_other_tables(list_clinical_table_names):
    df_demographic = extract_rc_demographic_clean()
    df_clinical = extract_rc_clinical()

    date_format = "%Y-%m-%d"
    dict_clinical_info_tables = {}

    for clinical_table_name in list_clinical_table_names:
        dict_clinical_info_tables[clinical_table_name] = get_table_from_database(clinical_table_name)

    if 'rc_ataxia_pd_scales' in list_clinical_table_names:
        df_ataxia_pd_scales_clean = extract_rc_ataxia_pd_scales_clean()
        return df_demographic, df_clinical, df_ataxia_pd_scales_clean, dict_clinical_info_tables
    else:
        return df_demographic, df_clinical, None, dict_clinical_info_tables

dtype_mapping = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "float64": "DOUBLE PRECISION",
    "float32": "REAL",
    "bool": "BOOLEAN",
    "object": "TEXT",
    "string": "TEXT",
    "datetime64[ns]": "TIMESTAMP",
    "timedelta[ns]": "INTERVAL"
}

def map_dtype(dtype):
    dtype_str = str(dtype)
    return dtype_mapping.get(dtype_str, "TEXT")  # default to TEXT if unknown

def create_new_table(table_name, df, list_dtypes):
    with SSHTunnelForwarder(**ssh_args) as tunnel:
        with psycopg2.connect(**db_args) as conn:
            
            list_nb_tables = neurobooth_terra.list_tables(conn)
            if table_name in list_nb_tables:
                neurobooth_terra.drop_table(table_name, conn)

            neurobooth_terra.create_table(table_name, conn, df.columns, list_dtypes, primary_key=['patient_id', 'session_date'])
            Table(table_name, conn).insert_rows(df.to_records(index=False).tolist(), df.columns)

def get_table_from_database(table_name):
    with SSHTunnelForwarder(**ssh_args) as tunnel:
        with psycopg2.connect(**db_args) as conn:
            df = Table(table_name, conn).query()

    return df

def get_table_from_query(query, list_column_names):
    with SSHTunnelForwarder(**ssh_args) as tunnel:
        with psycopg2.connect(**db_args) as conn:
            df = neurobooth_terra.query(conn, query, list_column_names)

    return df


if __name__ == '__main__':
    df_subject_id_session_date = get_table_from_query('SELECT subject_id, date from log_session', ['subject_id', 'date'])
    df_patients = get_table_from_query('SELECT subject_id from rc_baseline_data WHERE test_subject_boolean=False', ['subject_id'])
    df_subject_id_session_date = df_subject_id_session_date[df_subject_id_session_date['subject_id'].isin(df_patients['subject_id'])]
	
    list_clinical_table_names = ['rc_ataxia_pd_scales', 'rc_alsfrs', 'rc_dysarthria_impact_scale', 'rc_communicative_participation_item_bank', 
                            'rc_neuro_qol_anxiety_short_form', 'rc_neuro_qol_cognitive_function_short_form', 
                            'rc_neuro_qol_depression_short_form', 'rc_neuro_qol_emotional_dyscontrol_short_form', 
                            'rc_neuro_qol_fatigue_short_form', 'rc_neuro_qol_le_short_form',
                            'rc_neuro_qol_participate_social_roles_short_form', 'rc_neuro_qol_positive_affect_and_wellbeing_short_form',
                            'rc_neuro_qol_sleep_disturbance_short_form', 'rc_neuro_qol_ue_short_form']

    list_columns_to_remove = []
    date_format = "%Y-%m-%d"
    date_now = datetime.now().strftime("%m-%d-%Y")

    df_demographic, df_clinical, df_ataxia_pd_scales_clean, dict_clinical_info_tables = extract_demographic_clinical_and_other_tables(list_clinical_table_names)

    list_patient_IDs = df_subject_id_session_date['subject_id']
    list_session_dates = df_subject_id_session_date['date'].astype('string')
    report_clinical_info, all_missing_clinical_info = get_specific_clinical_information(df_demographic, df_clinical, df_ataxia_pd_scales_clean, dict_clinical_info_tables, list_clinical_table_names, list_patient_IDs, list_session_dates, date_format)
    
    pd_clinical_info = pd.DataFrame.from_dict(report_clinical_info)
    pd_clinical_info.drop_duplicates(subset=['patient_id', 'session_date'], keep='first', inplace=True)


    list_dtypes = []

    for col, dtype in pd_clinical_info.dtypes.items():
        if 'time_diff' in col:
            pd_clinical_info[col] = pd_clinical_info[col].apply(lambda x: x.days)
            list_dtypes.append("DOUBLE PRECISION")
        elif col == 'end_time_ataxia_pd_scales':
            list_dtypes.append("BIGINT")
        else:
            list_dtypes.append(map_dtype(dtype))

    pd_clinical_info.to_csv('neurobooth_clinical.csv', index=False)
    # pd_clinical_info.to_csv('_clinical_info.csv', index=False)

    list_nb_tables = neurobooth_terra.list_tables(conn)
    if table_name in list_nb_tables:
        neurobooth_terra.drop_table(table_name, conn)

    neurobooth_terra.create_table(table_name, conn, df.columns, list_dtypes, primary_key=['patient_id', 'session_date'])
    Table(table_name, conn).insert_rows(df.to_records(index=False).tolist(), df.columns)

    create_new_table('neurobooth_clinical', pd_clinical_info, list_dtypes)
    # pd_missing_clinical_info = pd.DataFrame.from_dict(all_missing_clinical_info)
    # pd_missing_clinical_info.to_csv(os.path.join(output_folder, date_now + '_missing_clinical_info.csv'), index=False)