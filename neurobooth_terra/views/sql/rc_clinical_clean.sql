CREATE OR REPLACE VIEW rc_clinical_clean AS
WITH last_clin AS (  -- Temporary view that isolates the latest entry in the clinical table
    SELECT DISTINCT ON (subject_id)
		subject_id,
		primary_diagnosis
	FROM rc_clinical
	ORDER BY
		subject_id,
		date_enrolled DESC
)
SELECT
    -- ========================================
    -- METADATA
    -- ========================================
    clin.subject_id,
    clin.redcap_event_name,
    (regexp_match(clin.redcap_event_name, 'v(\d+)_arm_\d+'))[1]::int AS redcap_sequence_num,
	(regexp_match(clin.redcap_event_name, 'v\d+_arm_(\d+)'))[1]::int AS redcap_study_arm,
    CASE
        WHEN clin.clinical_complete = 2 THEN TRUE
        ELSE FALSE
    END AS clinical_complete,
    clin.date_enrolled,
    clin.start_time_clinical,
    clin.end_time_clinical,
    CASE
        WHEN clin.neurologist = 99 THEN clin.other_neurologist
        WHEN clin.neurologist IS NOT NULL THEN ( -- Look up associated neurologist from the data dictionary
            SELECT dd.response_array->>clin.neurologist::text FROM rc_data_dictionary dd
            WHERE dd.database_table_name = 'clinical' AND dd.field_name = 'neurologist'
        )
    END AS neurologist,

    -- ========================================
    -- Biometrics
    -- ========================================
    clin.height,  -- TODO: Ask RCs to convert to float
    clin.weight,  -- TODO: Ask RCs to convert to float

    -- ========================================
    -- Diagnoses
    -- ========================================
    -- Here Be Dragons: The PostgreSQL syntax for handling arrays and json objects is akin to dark magic
    -- ARRAY(SELECT ... FROM UNNEST(array_column)) allows a transformation to be applied to each element of an array
    -- To look up diagnosis codes in the data dictionary (response_array->>diag::text), we:
    --   1. Convert the diagnosis code to a text type to get the dictionary key: (diag::text)
    --   2. Look up the key in the json dictionary and return it as a text type (response_array->>...)
    ARRAY(
        SELECT CASE
            WHEN diag = 5  THEN clin.other_dementia
            WHEN diag = 13 THEN clin.other_neuropathy
            WHEN diag = 23 THEN clin.other_ataxia
            WHEN diag = 24 THEN clin.other_primary_diagnosis
            ELSE (
                SELECT dd.response_array->>diag::text FROM rc_data_dictionary dd
                WHERE dd.database_table_name = 'clinical' AND dd.field_name = 'primary_diagnosis'
            )
        END
        FROM UNNEST(clin.primary_diagnosis) as diag
    ) as primary_diagnosis_at_visit,
    clin.primary_diagnosis AS primary_diagnosis_id_at_visit,
    ARRAY( -- Same as above, but for last_clin...
        SELECT CASE
            WHEN diag = 5  THEN clin.other_dementia
            WHEN diag = 13 THEN clin.other_neuropathy
            WHEN diag = 23 THEN clin.other_ataxia
            WHEN diag = 24 THEN clin.other_primary_diagnosis
            ELSE (
                SELECT dd.response_array->>diag::text FROM rc_data_dictionary dd
                WHERE dd.database_table_name = 'clinical' AND dd.field_name = 'primary_diagnosis'
            )
        END
        FROM UNNEST(last_clin.primary_diagnosis) as diag
    ) as latest_primary_diagnosis,
    last_clin.primary_diagnosis AS latest_primary_diagnosis_id,
    clin.year_primary_diagnosis,
    clin.secondary_diagnosis,
    clin.diagnosis_notes,
    clin.year_secondary_diagnosis,
    clin.age_symptom_onset,
    clin.first_symptom,
    clin.genetic_testing_boolean,
    clin.mri_boolean,

    -- ========================================
    -- Comorbidities
    -- ========================================
    clin.additional_medical_conditions,
    clin.musculoskeletal_conditions,
    clin.limb_movement_conditions,
    clin.walking_conditions,
    clin.balance_conditions,
    clin.speech_conditions,
    clin.vision_conditions,
    clin.hearing_conditions,
    clin.assistive_walking_device_boolean,
    clin.assistive_walking_device_type,
    clin.assistive_walking_device_percent_time_usage,
    clin.stroke_boolean,
    clin.neuropathy_boolean,
    clin.dyslexia_boolean,

    -- ========================================
    -- Medications
    -- ========================================
    clin.current_medications,
    clin.previous_medications,

    -- ========================================
    -- Participation in Other Studies
    -- ========================================
    clin.harvard_biomarker_boolean,
    clin.biobank_boolean,
    1 = ANY(clin.drug_trial_past_or_present) AS drug_trial_present,
    2 = ANY(clin.drug_trial_past_or_present)  AS drug_trial_past,
    clin.drug_trial_name

FROM rc_clinical clin
JOIN last_clin
    ON last_clin.subject_id = clin.subject_id
ORDER BY
    clin.subject_id,
    clin.redcap_event_name
;
