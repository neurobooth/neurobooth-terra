-- ================================================================================
-- Reflects the subset of rc_ataxia_pd_scales_clean related to SARA
-- Any "business logic" should likely be performed in the source view.
-- ================================================================================

CREATE OR REPLACE VIEW v_scale_sara AS
SELECT
    -- ========================================
    -- METADATA
    -- ========================================
    scales.subject_id,
    scales.redcap_event_name,
    scales.ataxia_pd_scales_complete,
    scales.visit_date,
    scales.end_time_ataxia_pd_scales,
    scales.in_person_boolean,
    scales.rater_name,
    scales.comments_ataxia_pd_scales,

    -- ========================================
    -- SARA
    -- ========================================
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
--     scales.sara_total_score,

    -- ========================================
    -- Comments
    -- ========================================
    scales.sara_gait_comments,
    scales.sara_stance_comments,
    scales.sara_sitting_comments,
    scales.sara_speech_disturbance_comments,
    scales.sara_finger_chase_comments,
    scales.sara_finger_nose_comments,
    scales.sara_alternating_hand_movements_comments,
    scales.sara_heel_shin_comments

FROM rc_ataxia_pd_scales_clean scales
WHERE scales.sara_performed_boolean = TRUE
