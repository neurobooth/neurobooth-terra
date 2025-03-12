WITH vd_dist AS (
    -- Figure out which demographic dates are closest (distance = 1)
    SELECT
        visit.subject_id,
        visit.log_session_id,
        visit.date AS visit_date,
        demog.end_time_demographic AS demog_date,
        visit.date - DATE(demog.end_time_demographic) AS offset_days,
        ROW_NUMBER() OVER (
                PARTITION BY visit.subject_id, visit.date
                ORDER BY ABS(visit.date - DATE(demog.end_time_demographic)) ASC
        ) AS distance
    FROM log_session visit
    FULL OUTER JOIN rc_demographic_clean demog
        ON visit.subject_id = demog.subject_id
    WHERE visit.date IS NOT NULL
), vc_dist AS (
    -- Figure out which clinic dates are closest (distance = 1)
    SELECT
        visit.subject_id,
        visit.neurobooth_visit_dates AS visit_date,
        clin.date_enrolled AS clin_date,
        visit.neurobooth_visit_dates - clin.date_enrolled AS offset_days,
        ROW_NUMBER() OVER (
                PARTITION BY visit.subject_id, visit.neurobooth_visit_dates
                ORDER BY ABS(visit.neurobooth_visit_dates - clin.date_enrolled) ASC
        ) AS distance
    FROM rc_visit_dates visit
    FULL OUTER JOIN rc_clinical_clean clin
        ON visit.subject_id = clin.subject_id
    WHERE visit.neurobooth_visit_dates IS NOT NULL
), vs_dist AS (
    -- Figure out which scale dates are closest (distance = 1)
    SELECT
        visit.subject_id,
        visit.neurobooth_visit_dates AS visit_date,
        scales.end_time_ataxia_pd_scales AS scale_date,
        visit.neurobooth_visit_dates - DATE(scales.end_time_ataxia_pd_scales) AS offset_days,
        ROW_NUMBER() OVER (
                PARTITION BY visit.subject_id, visit.neurobooth_visit_dates
                ORDER BY ABS(visit.neurobooth_visit_dates - DATE(scales.end_time_ataxia_pd_scales)) ASC
        ) AS distance
    FROM rc_visit_dates visit
    FULL OUTER JOIN rc_ataxia_pd_scales_clean scales
        ON visit.subject_id = scales.subject_id
    WHERE visit.neurobooth_visit_dates IS NOT NULL
), vprom_dist AS (
    -- Figure out which PROM-Ataxia are closest (distance = 1)
    SELECT
        visit.subject_id,
        visit.neurobooth_visit_dates AS visit_date,
        prom.end_time_prom_ataxia AS prom_date,
        visit.neurobooth_visit_dates - DATE(prom.end_time_prom_ataxia) AS offset_days,
        ROW_NUMBER() OVER (
                PARTITION BY visit.subject_id, visit.neurobooth_visit_dates
                ORDER BY ABS(visit.neurobooth_visit_dates - DATE(prom.end_time_prom_ataxia)) ASC
        ) AS distance
    FROM rc_visit_dates visit
    FULL OUTER JOIN rc_prom_ataxia prom
        ON visit.subject_id = prom.subject_id
    WHERE visit.neurobooth_visit_dates IS NOT NULL
)
SELECT
    -- Session Info
    subj.subject_id,
    vd_dist.log_session_id,
    COALESCE(baseline.test_subject_boolean, TRUE) AS test_subject_boolean,
    -- Join Info
    vd_dist.visit_date,
    DATE(vd_dist.demog_date) AS demog_date,
    vd_dist.offset_days AS demog_offset_days,
    demog.end_time_demographic IS NOT NULL AND ABS(vd_dist.offset_days) <= 60 AS recent_demog,
    vc_dist.clin_date,
    vc_dist.offset_days AS clin_offset_days,
    clin.date_enrolled IS NOT NULL AND ABS(vc_dist.offset_days) <= 60 AS recent_clin,
    DATE(vs_dist.scale_date) AS scale_date,
    vs_dist.offset_days AS scales_offset_days,
    scales.end_time_ataxia_pd_scales IS NOT NULL AND ABS(vs_dist.offset_days) <= 60 AS recent_scale,
    DATE(vprom_dist.prom_date) AS prom_date,
    vprom_dist.offset_days AS prom_offset_days,
    prom.end_time_prom_ataxia IS NOT NULL AND ABS(vprom_dist.offset_days) <= 60 AS recent_prom,
    -- Subject Demographics and Clinical Info
    EXTRACT(YEAR FROM AGE(vd_dist.visit_date, subj.date_of_birth_subject)) AS age,
    CASE
        WHEN CAST(CAST(subj.gender_at_birth AS FLOAT) AS INT) = 1 THEN 'Male'
        WHEN CAST(CAST(subj.gender_at_birth AS FLOAT) AS INT) = 2 THEN 'Female'
        ELSE '?'
    END AS gender_birth,
    demog.handedness,
    clin.primary_diagnosis_at_visit,
	clin.primary_diagnosis_id_at_visit,
    CASE  -- Case statement also serves to return false for NULL entries
        WHEN 0 = ANY(clin.primary_diagnosis_id_at_visit) THEN TRUE
        ELSE FALSE
    END AS is_control,
    LEFT(clin.remote_sca_id, -2) AS remote_sca_id,  -- removes the .0 from the end
    -- BARS
    scales.bars_gait,
    scales.bars_heel_shin_right,
    scales.bars_heel_shin_left,
    scales.bars_finger_nose_right,
    scales.bars_finger_nose_left,
    CASE
        WHEN demog.handedness = 'Left' THEN scales.bars_finger_nose_left
        ELSE scales.bars_finger_nose_right
    END AS bars_dom_arm,
    scales.bars_speech,
    scales.bars_oculomotor,
    scales.bars_total,
    -- UPDRS
	scales.updrs_speech,
    scales.updrs_facial_expressions,
    scales.updrs_rigidity_neck,
    scales.updrs_rigidity_rue,
    scales.updrs_rigidity_lue,
    scales.updrs_rigidity_rle,
    scales.updrs_rigidity_lle,
    scales.updrs_finger_tapping_right,
    scales.updrs_finger_tapping_left,
    scales.updrs_hand_movements_right,
    scales.updrs_hand_movements_left,
    scales.updrs_pronation_supination_right,
    scales.updrs_pronation_supination_left,
    scales.updrs_toe_tapping_right,
    scales.updrs_toe_tapping_left,
    scales.updrs_leg_agility_right,
    scales.updrs_leg_agility_left,
    scales.updrs_arising_from_chair,
    scales.updrs_gait,
    scales.updrs_gait_freeze,
    scales.updrs_postural_stability,
    scales.updrs_posture,
    scales.updrs_global_spontaneity_of_movement,
    scales.updrs_postural_tremor_right,
    scales.updrs_postural_tremor_left,
    scales.updrs_kinetic_tremor_right,
    scales.updrs_kinetic_tremor_left,
    scales.updrs_rest_tremor_rue,
    scales.updrs_rest_tremor_lue,
    scales.updrs_rest_tremor_rle,
    scales.updrs_rest_tremor_lle,
    scales.updrs_rest_tremor_lip_jaw,
    scales.updrs_tremor_constancy_of_rest_tremor,
    scales.updrs_dyskinesia,
    scales.updrs_dyskinesia_interfere,
    scales.updrs_total,
    -- SARA
    scales.sara_gait,
    scales.sara_stance,
    scales.sara_sitting,
    scales.sara_speech_disturbance,
    scales.sara_finger_chase_right,
    scales.sara_finger_chase_left,
    scales.sara_finger_nose_right,
    scales.sara_finger_nose_left,
    scales.sara_alternating_hand_movements_right,
    scales.sara_alternating_hand_movements_left,
    scales.sara_heel_shin_right,
    scales.sara_heel_shin_left,
    scales.sara_total,
    -- PROM Ataxia
    prom.prom_ataxia_1,
    prom.prom_ataxia_2,
    prom.prom_ataxia_3,
    prom.prom_ataxia_4,
    prom.prom_ataxia_5,
    prom.prom_ataxia_6,
    prom.prom_ataxia_7,
    prom.prom_ataxia_8,
    prom.prom_ataxia_9,
    prom.prom_ataxia_10,
    prom.prom_ataxia_11,
    prom.prom_ataxia_12,
    prom.prom_ataxia_13,
    prom.prom_ataxia_14,
    prom.prom_ataxia_15,
    prom.prom_ataxia_16,
    prom.prom_ataxia_17,
    prom.prom_ataxia_18,
    prom.prom_ataxia_19,
    prom.prom_ataxia_20,
    prom.prom_ataxia_21,
    prom.prom_ataxia_22,
    prom.prom_ataxia_23,
    prom.prom_ataxia_24,
    prom.prom_ataxia_25,
    prom.prom_ataxia_26,
    prom.prom_ataxia_27,
    prom.prom_ataxia_28,
    prom.prom_ataxia_29,
    prom.prom_ataxia_30,
    prom.prom_ataxia_31,
    prom.prom_ataxia_32,
    prom.prom_ataxia_33,
    prom.prom_ataxia_34,
    prom.prom_ataxia_35,
    prom.prom_ataxia_36,
    prom.prom_ataxia_37,
    prom.prom_ataxia_38,
    prom.prom_ataxia_39,
    prom.prom_ataxia_40,
    prom.prom_ataxia_41,
    prom.prom_ataxia_42,
    prom.prom_ataxia_43,
    prom.prom_ataxia_44,
    prom.prom_ataxia_45,
    prom.prom_ataxia_46,
    prom.prom_ataxia_47,
    prom.prom_ataxia_48,
    prom.prom_ataxia_49,
    prom.prom_ataxia_50,
    prom.prom_ataxia_51,
    prom.prom_ataxia_52,
    prom.prom_ataxia_53,
    prom.prom_ataxia_54,
    prom.prom_ataxia_55,
    prom.prom_ataxia_56,
    prom.prom_ataxia_57,
    prom.prom_ataxia_58,
    prom.prom_ataxia_59,
    prom.prom_ataxia_60,
    prom.prom_ataxia_61,
    prom.prom_ataxia_62,
    prom.prom_ataxia_63,
    prom.prom_ataxia_64,
    prom.prom_ataxia_65,
    prom.prom_ataxia_66,
    prom.prom_ataxia_67,
    prom.prom_ataxia_68,
    prom.prom_ataxia_69,
    prom.prom_ataxia_70,
    prom.total_prom_ataxia,
	prom.total_physical_section_1_prom_ataxia,
	prom.total_physical_section_2_prom_ataxia,
	prom.total_physical_prom_ataxia,
	prom.total_adl_prom_ataxia,
	prom.total_mental_section_1_prom_ataxia,
	prom.total_mental_section_2_prom_ataxia,
    prom.total_arm_prom_ataxia,
    prom.total_gait_prom_ataxia,
    prom.total_speech_prom_ataxia,
    prom.total_swallowing_prom_ataxia,
    prom.total_communication_prom_ataxia
FROM subject subj
LEFT JOIN rc_participant_and_consent_information consent
	ON subj.subject_id = consent.subject_id
LEFT JOIN rc_baseline_data baseline
	ON subj.subject_id = baseline.subject_id
-- Identify which RedCap entries are closest to the visit date
LEFT JOIN vd_dist
    ON subj.subject_id = vd_dist.subject_id
    AND vd_dist.distance = 1
LEFT JOIN vc_dist
    ON subj.subject_id = vc_dist.subject_id
    AND vc_dist.distance = 1
LEFT JOIN vs_dist
    ON subj.subject_id = vs_dist.subject_id
    AND vs_dist.distance = 1
LEFT JOIN vprom_dist
    ON subj.subject_id = vprom_dist.subject_id
    AND vprom_dist.distance = 1
LEFT JOIN rc_visit_dates visit
    ON subj.subject_id = visit.subject_id
    AND vd_dist.visit_date = visit.neurobooth_visit_dates
    AND vc_dist.visit_date = visit.neurobooth_visit_dates
    AND vs_dist.visit_date = visit.neurobooth_visit_dates
    AND vprom_dist.visit_date = visit.neurobooth_visit_dates
-- Get the RedCap data corresponding to identified entries
LEFT JOIN rc_demographic_clean demog
    ON subj.subject_id = demog.subject_id
    AND vd_dist.demog_date = demog.end_time_demographic
LEFT JOIN rc_clinical_clean clin
    ON subj.subject_id = clin.subject_id
    AND vc_dist.clin_date = clin.date_enrolled
LEFT JOIN rc_ataxia_pd_scales_clean scales
    ON subj.subject_id = scales.subject_id
    AND vs_dist.scale_date = scales.end_time_ataxia_pd_scales
LEFT JOIN rc_prom_ataxia prom
    ON subj.subject_id = prom.subject_id
    AND vprom_dist.prom_date = prom.end_time_prom_ataxia
WHERE visit.neurobooth_visit_dates IS NOT NULL
ORDER BY
    CAST(subj.subject_id AS INT) ASC,
    visit.neurobooth_visit_dates ASC