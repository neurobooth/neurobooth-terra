-- ================================================================================
-- Reflects the subset of rc_ataxia_pd_scales_clean related to UPDRS
-- Any "business logic" should likely be performed in the source view.
-- ================================================================================

CREATE OR REPLACE VIEW v_scale_updrs AS
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
    -- UPDRS
    -- ========================================
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
--     scales.updrs_total_score,

    -- ========================================
    -- Comments
    -- ========================================
    scales.updrs_speech_comments,
    scales.updrs_rigidity_comments,
    scales.updrs_finger_tapping_comments,
    scales.updrs_hand_movements_comments,
    scales.updrs_pronation_supination_comments,
    scales.updrs_toe_tapping_comments,
    scales.updrs_leg_agility_comments,
    scales.updrs_arising_from_chair_comments,
    scales.updrs_gait_comments,
    scales.updrs_gait_freeze_comments,
    scales.updrs_postural_stability_comments,
    scales.updrs_posture_comments,
    scales.updrs_global_spontaneity_of_movement_comments,
    scales.updrs_postural_tremor_comments,
    scales.updrs_kinetic_tremor_comments,
    scales.updrs_rest_tremor_comments,
    scales.updrs_tremor_constancy_of_rest_tremor_comments,
    scales.levodopa_minutes

FROM rc_ataxia_pd_scales_clean scales
WHERE scales.updrs_performed_boolean = TRUE
