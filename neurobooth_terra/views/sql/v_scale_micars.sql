-- ================================================================================
-- Reflects the subset of rc_ataxia_pd_scales_clean related to MICARS
-- Any "business logic" should likely be performed in the source view.
-- ================================================================================

CREATE OR REPLACE VIEW v_scale_micars AS
SELECT
    -- ========================================
    -- METADATA
    -- ========================================
    scales.subject_id,
    scales.redcap_event_name,
    scales.redcap_sequence_num,
    scales.redcap_study_arm,
    scales.ataxia_pd_scales_complete,
    scales.visit_date,
    scales.end_time_ataxia_pd_scales,
    scales.in_person_boolean,
    scales.rater_name,
    scales.comments_ataxia_pd_scales,

    -- ========================================
    -- MICARS
    -- ========================================
    scales.micars_rebound_right,
    scales.micars_rebound_left,
    scales.micars_overshoot_arms_right,
    scales.micars_overshoot_arms_left,
    scales.micars_spiral,
    scales.micars_dysarthria_fluency_of_speech,
    scales.micars_dysarthria_clarity_of_speech,
    scales.micars_dysarthria_alternating_syllables,
    scales.micars_nystagmus,
    scales.micars_abnormalities_ocular_pursuit,
    scales.micars_dysmetria_saccade,
    scales.micars_saccadic_intrusions,

    -- ========================================
    -- Comments
    -- ========================================
    scales.micars_arms_comments,
    scales.micars_speech_comments,
    scales.micars_oculomotor_comments

FROM rc_ataxia_pd_scales_clean scales
WHERE scales.micars_performed_boolean = TRUE
