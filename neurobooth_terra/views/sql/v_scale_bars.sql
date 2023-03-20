-- ================================================================================
-- Reflects the subset of rc_ataxia_pd_scales_clean related to BARS
-- Any "business logic" should likely be performed in the source view.
-- ================================================================================

CREATE OR REPLACE VIEW v_scale_bars AS
SELECT
    -- ========================================
    -- METADATA
    -- ========================================
    scales.subject_id,
    scales.redcap_event_name,
    scales.ataxia_pd_scales_complete,
    scales.end_time_ataxia_pd_scales,
    scales.in_person_boolean,
    scales.rater_name,
    scales.comments_ataxia_pd_scales,

    -- ========================================
    -- BARS
    -- ========================================
    scales.bars_gait,
    scales.bars_heel_shin_right,
    scales.bars_heel_shin_left,
    scales.bars_finger_nose_right,
    scales.bars_finger_nose_left,
    scales.bars_speech,
    scales.bars_oculomotor,
--     scales.bars_total_score / 10 AS bars_total,

    -- ========================================
    -- Comments
    -- ========================================
    scales.bars_gait_comments,
    scales.bars_heel_shin_comments,
    scales.bars_finger_nose_comments,
    scales.bars_speech_comments,
    scales.bars_oculomotor_comments

FROM rc_ataxia_pd_scales_clean scales
WHERE scales.bars_performed_boolean = TRUE
