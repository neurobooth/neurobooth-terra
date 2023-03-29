-- Here Be Dragons: The PostgreSQL syntax for handling arrays and json objects is akin to dark magic
-- ARRAY(SELECT ... FROM UNNEST(array_column)) allows a transformation to be applied to each element of an array
-- To look up values in the data dictionary (response_array->>x::text), we:
--   1. Convert the integer to a text type to get the dictionary key: (x::text)
--   2. Look up the key in the json dictionary and return it as a text type (response_array->>...)

CREATE OR REPLACE VIEW rc_demographic_clean AS
SELECT
    -- ========================================
    -- METADATA
    -- ========================================
    pinfo.subject_id,
    dem.redcap_event_name,
    pinfo.test_subject_boolean,
    CASE
        WHEN dem.demographic_complete = 2 THEN TRUE
        ELSE FALSE
    END AS demographic_complete,
    dem.start_time_demographic,
    dem.end_time_demographic,

    -- ========================================
    -- Physical Characteristics
    -- ========================================
    pinfo.subject_age_first_contact AS age_first_contact,
    CASE
        WHEN dem.gender_current = 7 THEN NULL
        ELSE ( -- Look up from the data dictionary
            SELECT dd.response_array->>dem.gender_current::text FROM rc_data_dictionary dd
            WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'gender_current'
        )
    END AS gender,
    ( -- Look up from the data dictionary
        SELECT dd.response_array->>dem.handedness::text FROM rc_data_dictionary dd
        WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'handedness'
    ) AS handedness,
    ARRAY(
        SELECT CASE
            WHEN x = 7 THEN NULL
            ELSE ( -- Look up from the data dictionary
                SELECT dd.response_array->>x::text FROM rc_data_dictionary dd
                WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'race'
        ) END
        FROM UNNEST(dem.race) as x
    ) AS race,
    ARRAY(
        SELECT CASE
            WHEN x = 12 THEN NULL
            ELSE ( -- Look up from the data dictionary
                SELECT dd.response_array->>x::text FROM rc_data_dictionary dd
                WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'ancestry'
        ) END
        FROM UNNEST(dem.ancestry) as x
    ) AS ancestry,
    CASE
        WHEN dem.ethnicity = 3 THEN NULL
        ELSE ( -- Look up from the data dictionary
            SELECT dd.response_array->>dem.ethnicity::text FROM rc_data_dictionary dd
            WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'ethnicity'
        )
    END AS ethnicity,

    -- ========================================
    -- Health
    -- ========================================
    ARRAY(
        SELECT ( -- Look up from the data dictionary
            SELECT dd.response_array->>x::text FROM rc_data_dictionary dd
            WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'health_history'
        ) FROM UNNEST(dem.health_history) as x
    ) AS health_history,
    dem.smoker_boolean,
    dem.years_smoking,
    dem.packs_per_day,
    dem.date_last_smoked,

    -- ========================================
    -- Familiarity with Technology
    -- ========================================
    dem.smartphone_owner_boolean,
    ( -- Look up from the data dictionary
        SELECT dd.response_array->>dem.smartphone_ease_of_usage::text FROM rc_data_dictionary dd
        WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'smartphone_ease_of_usage'
    ) AS smartphone_ease_of_usage,
    dem.internet_access_boolean,

    -- ========================================
    -- Language
    -- ========================================
    CASE
        WHEN dem.primary_language = 11 THEN dem.primary_language_other
        WHEN dem.primary_language IS NOT NULL THEN ( -- Look up from the data dictionary
            SELECT dd.response_array->>dem.primary_language::text FROM rc_data_dictionary dd
            WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'primary_language'
        )
    END AS primary_language,
    dem.primary_language_dialect,
    CASE
        WHEN dem.first_language = 11 THEN dem.first_language_other
        WHEN dem.first_language IS NOT NULL THEN ( -- Look up from the data dictionary
            SELECT dd.response_array->>dem.first_language::text FROM rc_data_dictionary dd
            WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'first_language'
        )
    END AS first_language,
    dem.first_language_dialect,
    dem.additional_languages,

    -- ========================================
    -- Location
    -- ========================================
    dem.zipcode,

    -- ========================================
    -- Education, Occupation, Marriage
    -- ========================================
    ( -- Look up from the data dictionary
        SELECT dd.response_array->>dem.education::text FROM rc_data_dictionary dd
        WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'education'
    ) AS education,
    ( -- Look up from the data dictionary
        SELECT dd.response_array->>dem.employment::text FROM rc_data_dictionary dd
        WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'employment'
    ) AS employment,
    dem.current_or_recent_job_title,
    dem.current_or_recent_employer,
    ( -- Look up from the data dictionary
        SELECT dd.response_array->>dem.marital_status::text FROM rc_data_dictionary dd
        WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'marital_status'
    ) AS marital_status,

    -- ========================================
    -- Recruitment and Comments
    -- ========================================
    dem.caretaker_boolean,
    dem.past_research_experience_boolean,
    CASE
        WHEN dem.recruitment_mechanism = 9 THEN dem.recruitment_mechanism_other
        WHEN dem.recruitment_mechanism IS NOT NULL THEN ( -- Look up from the data dictionary
            SELECT dd.response_array->>dem.recruitment_mechanism::text FROM rc_data_dictionary dd
            WHERE dd.database_table_name = 'demographic' AND dd.field_name = 'recruitment_mechanism'
        )
    END AS recruitment_mechanism,
    dem.comments_demographic

FROM rc_demographic dem
RIGHT OUTER JOIN rc_participant_and_consent_information pinfo
    ON dem.subject_id = pinfo.subject_id  -- Should by a many-to-one join
ORDER BY
    subject_id,
    redcap_event_name
;
