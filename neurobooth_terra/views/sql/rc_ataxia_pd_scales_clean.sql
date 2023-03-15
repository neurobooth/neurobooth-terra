CREATE OR REPLACE VIEW rc_ataxia_pd_scales_clean AS
SELECT
    -- ========================================
    -- METADATA
    -- ========================================
    scales.subject_id,
    scales.redcap_event_name,
    CASE
        WHEN ataxia_pd_scales_complete = 2 THEN TRUE
        ELSE FALSE
    END AS ataxia_pd_scales_complete,
    scales.end_time_ataxia_pd_scales,
    (scales.in_person_boolean_ataxia_pd_scales = 1) AS in_person_boolean,
    CASE
        WHEN scales.rater_ataxia_pd_scales = 1 THEN 'Jeremy Schmahmann'
        WHEN scales.rater_ataxia_pd_scales = 2 THEN 'Christopher Stephen'
        WHEN scales.rater_ataxia_pd_scales = 3 THEN 'Anoopum Gupta'
        WHEN scales.rater_ataxia_pd_scales = 4 THEN scales.ataxia_pd_other_rater
        WHEN scales.rater_ataxia_pd_scales = 5 THEN 'Albert Hung'
        WHEN scales.rater_ataxia_pd_scales = 6 THEN 'Anne-Marie Wills'
    END AS rater_name,
    scales.comments_ataxia_pd_scales,

    -- ========================================
    -- BARS
    -- ========================================
    1 = ANY(scales.ataxia_pd_assessments_performed) AS bars_performed_boolean,
    CASE
        WHEN scales.bars_gait > 95 THEN NULL
        ELSE CAST(scales.bars_gait AS FLOAT) / 10
    END AS bars_gait,
    CASE
        WHEN scales.bars_heel_shin_right > 95 THEN NULL
        ELSE CAST(scales.bars_heel_shin_right AS FLOAT) / 10
    END AS bars_heel_shin_right,
    CASE
        WHEN scales.bars_heel_shin_left > 95 THEN NULL
        ELSE CAST(scales.bars_heel_shin_left AS FLOAT) / 10
    END AS bars_heel_shin_left,
    CASE
        WHEN scales.bars_finger_nose_right > 95 THEN NULL
        ELSE CAST(scales.bars_finger_nose_right AS FLOAT) / 10
    END AS bars_finger_nose_right,
    CASE
        WHEN scales.bars_finger_nose_left > 95 THEN NULL
        ELSE CAST(scales.bars_finger_nose_left AS FLOAT) / 10
    END AS bars_finger_nose_left,
    CASE
        WHEN scales.bars_speech > 95 THEN NULL
        ELSE CAST(scales.bars_speech AS FLOAT) / 10
    END AS bars_speech,
    CASE
        WHEN scales.bars_oculomotor > 95 THEN NULL
        ELSE CAST(scales.bars_oculomotor AS FLOAT) / 10
    END AS bars_oculomotor,
    scales.bars_total_score / 10 AS bars_total,

    -- ========================================
    -- BARS Comments
    -- ========================================
    scales.bars_gait_comments,
    scales.bars_heel_shin_comments,
    scales.bars_finger_nose_comments,
    scales.bars_speech_comments,
    scales.bars_oculomotor_comments,

    -- ========================================
    -- SARA
    -- ========================================
    2 = ANY(scales.ataxia_pd_assessments_performed) AS sara_performed_boolean,
    CASE
        WHEN scales.sara_gait_1 > 95 THEN NULL
        ELSE scales.sara_gait_1
    END AS sara_gait,
    CASE
        WHEN scales.sara_stance_2 > 95 THEN NULL
        ELSE scales.sara_stance_2
    END AS sara_stance,
    CASE
        WHEN scales.sara_sitting_3 > 95 THEN NULL
        ELSE scales.sara_sitting_3
    END AS sara_sitting,
    CASE
        WHEN scales.sara_speech_disturbance_4 > 95 THEN NULL
        ELSE scales.sara_speech_disturbance_4
    END AS sara_speech_disturbance,
    CASE
        WHEN scales.sara_finger_chase_right_5 > 95 THEN NULL
        ELSE scales.sara_finger_chase_right_5
    END AS sara_finger_chase_right,
    CASE
        WHEN scales.sara_finger_chase_left_5 > 95 THEN NULL
        ELSE scales.sara_finger_chase_left_5
    END AS sara_finger_chase_left,
    CASE
        WHEN scales.sara_finger_nose_right_6 > 95 THEN NULL
        ELSE scales.sara_finger_nose_right_6
    END AS sara_finger_nose_right,
    CASE
        WHEN scales.sara_finger_nose_left_6 > 95 THEN NULL
        ELSE scales.sara_finger_nose_left_6
    END AS sara_finger_nose_left,
    CASE
        WHEN scales.sara_alternating_hand_movements_right_7 > 95 THEN NULL
        ELSE scales.sara_alternating_hand_movements_right_7
    END AS sara_alternating_hand_movements_right,
    CASE
        WHEN scales.sara_alternating_hand_movements_left_7 > 95 THEN NULL
        ELSE scales.sara_alternating_hand_movements_left_7
    END AS sara_alternating_hand_movements_left,
    CASE
        WHEN scales.sara_heel_shin_right_8 > 95 THEN NULL
        ELSE scales.sara_heel_shin_right_8
    END AS sara_heel_shin_right,
    CASE
        WHEN scales.sara_heel_shin_left_8 > 95 THEN NULL
        ELSE scales.sara_heel_shin_left_8
    END AS sara_heel_shin_left,
    scales.sara_total_score,

    -- ========================================
    -- SARA Comments
    -- ========================================
    scales.sara_gait_1_comments AS sara_gait_comments,
    scales.sara_stance_2_comments AS sara_stance_comments,
    scales.sara_sitting_3_comments AS sara_sitting_comments,
    scales.sara_speech_disturbance_4_comments AS sara_speech_disturbance_comments,
    scales.sara_finger_chase_5_comments AS sara_finger_chase_comments,
    scales.sara_finger_nose_6_comments AS sara_finger_nose_comments,
    scales.sara_alternating_hand_movements_7_comments AS sara_alternating_hand_movements_comments,
    scales.sara_heel_shin_comments AS sara_heel_shin_comments,

    -- ========================================
    -- MICARS
    -- ========================================
    4 = ANY(scales.ataxia_pd_assessments_performed) AS micars_performed_boolean,
    CASE
        WHEN scales.micars_rebound_right_16 > 95 THEN NULL
        ELSE scales.micars_rebound_right_16
    END AS micars_rebound_right,
    CASE
        WHEN scales.micars_rebound_left_16 > 95 THEN NULL
        ELSE scales.micars_rebound_left_16
    END AS micars_rebound_left,
    CASE
        WHEN scales.micars_overshoot_arms_right_17 > 95 THEN NULL
        ELSE scales.micars_overshoot_arms_right_17
    END AS micars_overshoot_arms_right,
    CASE
        WHEN scales.micars_overshoot_arms_left_17 > 95 THEN NULL
        ELSE scales.micars_overshoot_arms_left_17
    END AS micars_overshoot_arms_left,
    CASE
        WHEN scales.micars_spiral_18 > 95 THEN NULL
        ELSE scales.micars_spiral_18
    END AS micars_spiral,
    CASE
        WHEN scales.micars_dysarthria_fluency_of_speech_19 > 95 THEN NULL
        ELSE scales.micars_dysarthria_fluency_of_speech_19
    END AS micars_dysarthria_fluency_of_speech,
    CASE
        WHEN scales.micars_dysarthria_clarity_of_speech_20 > 95 THEN NULL
        ELSE scales.micars_dysarthria_clarity_of_speech_20
    END AS micars_dysarthria_clarity_of_speech,
    CASE
        WHEN scales.micars_dysarthria_alternating_syllables_21 > 95 THEN NULL
        ELSE scales.micars_dysarthria_alternating_syllables_21
    END AS micars_dysarthria_alternating_syllables,
    CASE
        WHEN scales.micars_nystagmus_23 > 95 THEN NULL
        ELSE scales.micars_nystagmus_23
    END AS micars_nystagmus,
    CASE
        WHEN scales.micars_abnormalities_ocular_pursuit_24 > 95 THEN NULL
        ELSE scales.micars_abnormalities_ocular_pursuit_24
    END AS micars_abnormalities_ocular_pursuit,
    CASE
        WHEN scales.micars_dysmetria_saccade_25 > 95 THEN NULL
        ELSE scales.micars_dysmetria_saccade_25
    END AS micars_dysmetria_saccade,
    CASE
        WHEN scales.micars_saccadic_intrusions_26 > 95 THEN NULL
        ELSE scales.micars_saccadic_intrusions_26
    END AS micars_saccadic_intrusions,

    -- ========================================
    -- MICARS Comments
    -- ========================================
    scales.micars_arms_comments,
    scales.micars_speech_comments,
    scales.micars_oculomotor_comments,

    -- ========================================
    -- UPDRS
    -- ========================================
    3 = ANY(scales.ataxia_pd_assessments_performed) AS updrs_performed_boolean,
    CASE
        WHEN scales.updrs_speech_1 > 95 THEN NULL
        ELSE scales.updrs_speech_1
    END AS updrs_speech,
    CASE
        WHEN scales.updrs_facial_expressions_2 > 95 THEN NULL
        ELSE scales.updrs_facial_expressions_2
    END AS updrs_facial_expressions,
    CASE
        WHEN scales.updrs_rigidity_neck_3 > 95 THEN NULL
        ELSE scales.updrs_rigidity_neck_3
    END AS updrs_rigidity_neck,
    CASE
        WHEN scales.updrs_rigidity_rue_3 > 95 THEN NULL
        ELSE scales.updrs_rigidity_rue_3
    END AS updrs_rigidity_rue,
    CASE
        WHEN scales.updrs_rigidity_lue_3 > 95 THEN NULL
        ELSE scales.updrs_rigidity_lue_3
    END AS updrs_rigidity_lue,
    CASE
        WHEN scales.updrs_rigidity_rle_3 > 95 THEN NULL
        ELSE scales.updrs_rigidity_rle_3
    END AS updrs_rigidity_rle,
    CASE
        WHEN scales.updrs_rigidity_lle_3 > 95 THEN NULL
        ELSE scales.updrs_rigidity_lle_3
    END AS updrs_rigidity_lle,
    CASE
        WHEN scales.updrs_finger_tapping_right_4 > 95 THEN NULL
        ELSE scales.updrs_finger_tapping_right_4
    END AS updrs_finger_tapping_right,
    CASE
        WHEN scales.updrs_finger_tapping_left_4 > 95 THEN NULL
        ELSE scales.updrs_finger_tapping_left_4
    END AS updrs_finger_tapping_left,
    CASE
        WHEN scales.updrs_hand_movements_right_5 > 95 THEN NULL
        ELSE scales.updrs_hand_movements_right_5
    END AS updrs_hand_movements_right,
    CASE
        WHEN scales.updrs_hand_movements_left_5 > 95 THEN NULL
        ELSE scales.updrs_hand_movements_left_5
    END AS updrs_hand_movements_left,
    CASE
        WHEN scales.updrs_pronation_supination_right_6 > 95 THEN NULL
        ELSE scales.updrs_pronation_supination_right_6
    END AS updrs_pronation_supination_right,
    CASE
        WHEN scales.updrs_pronation_supination_left_6 > 95 THEN NULL
        ELSE scales.updrs_pronation_supination_left_6
    END AS updrs_pronation_supination_left,
    CASE
        WHEN scales.updrs_toe_tapping_right_7 > 95 THEN NULL
        ELSE scales.updrs_toe_tapping_right_7
    END AS updrs_toe_tapping_right,
    CASE
        WHEN scales.updrs_toe_tapping_left_7 > 95 THEN NULL
        ELSE scales.updrs_toe_tapping_left_7
    END AS updrs_toe_tapping_left,
    CASE
        WHEN scales.updrs_leg_agility_right_8 > 95 THEN NULL
        ELSE scales.updrs_leg_agility_right_8
    END AS updrs_leg_agility_right,
    CASE
        WHEN scales.updrs_leg_agility_left_8 > 95 THEN NULL
        ELSE scales.updrs_leg_agility_left_8
    END AS updrs_leg_agility_left,
    CASE
        WHEN scales.updrs_arising_from_chair_9 > 95 THEN NULL
        ELSE scales.updrs_arising_from_chair_9
    END AS updrs_arising_from_chair,
    CASE
        WHEN scales.updrs_gait_10 > 95 THEN NULL
        ELSE scales.updrs_gait_10
    END AS updrs_gait,
    CASE
        WHEN scales.updrs_gait_freeze_11 > 95 THEN NULL
        ELSE scales.updrs_gait_freeze_11
    END AS updrs_gait_freeze,
    CASE
        WHEN scales.updrs_postural_stability_12 > 95 THEN NULL
        ELSE scales.updrs_postural_stability_12
    END AS updrs_postural_stability,
    CASE
        WHEN scales.updrs_posture_13 > 95 THEN NULL
        ELSE scales.updrs_posture_13
    END AS updrs_posture,
    CASE
        WHEN scales.updrs_global_spontaneity_of_movement_14 > 95 THEN NULL
        ELSE scales.updrs_global_spontaneity_of_movement_14
    END AS updrs_global_spontaneity_of_movement,
    CASE
        WHEN scales.updrs_postural_tremor_right_15 > 95 THEN NULL
        ELSE scales.updrs_postural_tremor_right_15
    END AS updrs_postural_tremor_right,
    CASE
        WHEN scales.updrs_postural_tremor_left_15 > 95 THEN NULL
        ELSE scales.updrs_postural_tremor_left_15
    END AS updrs_postural_tremor_left,
    CASE
        WHEN scales.updrs_kinetic_tremor_right_16 > 95 THEN NULL
        ELSE scales.updrs_kinetic_tremor_right_16
    END AS updrs_kinetic_tremor_right,
    CASE
        WHEN scales.updrs_kinetic_tremor_left_16 > 95 THEN NULL
        ELSE scales.updrs_kinetic_tremor_left_16
    END AS updrs_kinetic_tremor_left,
    CASE
        WHEN scales.updrs_rest_tremor_rue_17 > 95 THEN NULL
        ELSE scales.updrs_rest_tremor_rue_17
    END AS updrs_rest_tremor_rue,
    CASE
        WHEN scales.updrs_rest_tremor_lue_17 > 95 THEN NULL
        ELSE scales.updrs_rest_tremor_lue_17
    END AS updrs_rest_tremor_lue,
    CASE
        WHEN scales.updrs_rest_tremor_rle_17 > 95 THEN NULL
        ELSE scales.updrs_rest_tremor_rle_17
    END AS updrs_rest_tremor_rle,
    CASE
        WHEN scales.updrs_rest_tremor_lle_17 > 95 THEN NULL
        ELSE scales.updrs_rest_tremor_lle_17
    END AS updrs_rest_tremor_lle,
    CASE
        WHEN scales.updrs_rest_tremor_lip_jaw_17 > 95 THEN NULL
        ELSE scales.updrs_rest_tremor_lip_jaw_17
    END AS updrs_rest_tremor_lip_jaw,
    CASE
        WHEN scales.updrs_tremor_constancy_of_rest_tremor_18 > 95 THEN NULL
        ELSE scales.updrs_tremor_constancy_of_rest_tremor_18
    END AS updrs_tremor_constancy_of_rest_tremor,
    scales.updrs_dyskinesia_boolean AS updrs_dyskinesia,
    scales.updrs_dyskinesia_interfere,
    scales.updrs_total_score,

    -- ========================================
    -- UPDRS Comments
    -- ========================================
    scales.updrs_speech_1_comments AS updrs_speech_comments,
    scales.updrs_rigidity_comments_3 AS updrs_rigidity_comments,
    scales.updrs_finger_tapping_4_comments AS updrs_finger_tapping_comments,
    scales.updrs_hand_movements_5_comments AS updrs_hand_movements_comments,
    scales.updrs_pronation_supination_6_comments AS updrs_pronation_supination_comments,
    scales.updrs_toe_tapping_7_comments AS updrs_toe_tapping_comments,
    scales.updrs_leg_agility_8_comments AS updrs_leg_agility_comments,
    scales.updrs_arising_from_chair_9_comments AS updrs_arising_from_chair_comments,
    scales.updrs_gait_10_comments AS updrs_gait_comments,
    scales.updrs_gait_freeze_11_comments AS updrs_gait_freeze_comments,
    scales.updrs_postural_stability_12_comments AS updrs_postural_stability_comments,
    scales.updrs_posture_13_comments AS updrs_posture_comments,
    scales.updrs_global_spontaneity_of_movement_14_comments AS updrs_global_spontaneity_of_movement_comments,
    scales.updrs_postural_tremor_15_comments AS updrs_postural_tremor_comments,
    scales.updrs_kinetic_tremor_16_comments AS updrs_kinetic_tremor_comments,
    scales.updrs_rest_tremor_17_comments AS updrs_rest_tremor_comments,
    scales.updrs_tremor_constancy_of_rest_tremor_18_comments AS updrs_tremor_constancy_of_rest_tremor_comments,
    scales.levodopa_minutes_ataxia_pd_scales AS levodopa_minutes,

    -- ========================================
    -- UHDRS
    -- ========================================
    5 = ANY(scales.ataxia_pd_assessments_performed) AS uhdrs_performed_boolean,
    CASE
        WHEN scales.uhdrs_pursuit_horizontal_1a > 95 THEN NULL
        ELSE scales.uhdrs_pursuit_horizontal_1a
    END AS uhdrs_pursuit_horizontal,
    CASE
        WHEN scales.uhdrs_pursuit_vertical_1b > 95 THEN NULL
        ELSE scales.uhdrs_pursuit_vertical_1b
    END AS uhdrs_pursuit_vertical,
    CASE
        WHEN scales.uhdrs_saccade_initation_horizontal_2a > 95 THEN NULL
        ELSE scales.uhdrs_saccade_initation_horizontal_2a
    END AS uhdrs_saccade_initation_horizontal,
    CASE
        WHEN scales.uhdrs_saccade_initation_vertical_2b > 95 THEN NULL
        ELSE scales.uhdrs_saccade_initation_vertical_2b
    END AS uhdrs_saccade_initation_vertical,
    CASE
        WHEN scales.uhdrs_saccade_velocity_horizontal_3a > 95 THEN NULL
        ELSE scales.uhdrs_saccade_velocity_horizontal_3a
    END AS uhdrs_saccade_velocity_horizontal,
    CASE
        WHEN scales.uhdrs_saccade_velocity_vertical_3b > 95 THEN NULL
        ELSE scales.uhdrs_saccade_velocity_vertical_3b
    END AS uhdrs_saccade_velocity_vertical,
    CASE
        WHEN scales.uhdrs_dysarthria_4 > 95 THEN NULL
        ELSE scales.uhdrs_dysarthria_4
    END AS uhdrs_dysarthria,
    CASE
        WHEN scales.uhdrs_tongue_protrusion_5 > 95 THEN NULL
        ELSE scales.uhdrs_tongue_protrusion_5
    END AS uhdrs_tongue_protrusion,
    CASE
        WHEN scales.uhdrs_finger_taps_right_6a > 95 THEN NULL
        ELSE scales.uhdrs_finger_taps_right_6a
    END AS uhdrs_finger_taps_right,
    CASE
        WHEN scales.uhdrs_finger_taps_left_6b > 95 THEN NULL
        ELSE scales.uhdrs_finger_taps_left_6b
    END AS uhdrs_finger_taps_left,
    CASE
        WHEN scales.uhdrs_pronate_supinate_hands_right_7a > 95 THEN NULL
        ELSE scales.uhdrs_pronate_supinate_hands_right_7a
    END AS uhdrs_pronate_supinate_hands_right,
    CASE
        WHEN scales.uhdrs_pronate_supinate_hands_left_7b > 95 THEN NULL
        ELSE scales.uhdrs_pronate_supinate_hands_left_7b
    END AS uhdrs_pronate_supinate_hands_left,
    CASE
        WHEN scales.uhdrs_luria > 95 THEN NULL
        ELSE scales.uhdrs_luria
    END AS uhdrs_luria,
    CASE
        WHEN scales.uhdrs_rigidity_arms_right_9a > 95 THEN NULL
        ELSE scales.uhdrs_rigidity_arms_right_9a
    END AS uhdrs_rigidity_arms_right,
    CASE
        WHEN scales.uhdrs_rigidity_arms_left_9b > 95 THEN NULL
        ELSE scales.uhdrs_rigidity_arms_left_9b
    END AS uhdrs_rigidity_arms_left,
    CASE
        WHEN scales.uhdrs_bradykinesia_body > 95 THEN NULL
        ELSE scales.uhdrs_bradykinesia_body
    END AS uhdrs_bradykinesia_body,
    CASE
        WHEN scales.uhdrs_maximal_dystonia_trunk_11a > 95 THEN NULL
        ELSE scales.uhdrs_maximal_dystonia_trunk_11a
    END AS uhdrs_maximal_dystonia_trunk,
    CASE
        WHEN scales.uhdrs_maximal_dystonia_rue_11b > 95 THEN NULL
        ELSE scales.uhdrs_maximal_dystonia_rue_11b
    END AS uhdrs_maximal_dystonia_rue,
    CASE
        WHEN scales.uhdrs_maximal_dystonia_lue_11c > 95 THEN NULL
        ELSE scales.uhdrs_maximal_dystonia_lue_11c
    END AS uhdrs_maximal_dystonia_lue,
    CASE
        WHEN scales.uhdrs_maximal_dystonia_rle_11d > 95 THEN NULL
        ELSE scales.uhdrs_maximal_dystonia_rle_11d
    END AS uhdrs_maximal_dystonia_rle,
    CASE
        WHEN scales.uhdrs_maximal_dystonia_lle_11e > 95 THEN NULL
        ELSE scales.uhdrs_maximal_dystonia_lle_11e
    END AS uhdrs_maximal_dystonia_lle,
    CASE
        WHEN scales.uhdrs_maximal_chorea_face_12a > 95 THEN NULL
        ELSE scales.uhdrs_maximal_chorea_face_12a
    END AS uhdrs_maximal_chorea_face,
    CASE
        WHEN scales.uhdrs_maximal_chorea_bol_12b > 95 THEN NULL
        ELSE scales.uhdrs_maximal_chorea_bol_12b
    END AS uhdrs_maximal_chorea_bol,
    CASE
        WHEN scales.uhdrs_maximal_chorea_trunk_12c > 95 THEN NULL
        ELSE scales.uhdrs_maximal_chorea_trunk_12c
    END AS uhdrs_maximal_chorea_trunk,
    CASE
        WHEN scales.uhdrs_maximal_chorea_rue_12d > 95 THEN NULL
        ELSE scales.uhdrs_maximal_chorea_rue_12d
    END AS uhdrs_maximal_chorea_rue,
    CASE
        WHEN scales.uhdrs_maximal_chorea_lue_12e > 95 THEN NULL
        ELSE scales.uhdrs_maximal_chorea_lue_12e
    END AS uhdrs_maximal_chorea_lue,
    CASE
        WHEN scales.uhdrs_maximal_chorea_rle_12f > 95 THEN NULL
        ELSE scales.uhdrs_maximal_chorea_rle_12f
    END AS uhdrs_maximal_chorea_rle,
    CASE
        WHEN scales.uhdrs_maximal_chorea_lle_12g > 95 THEN NULL
        ELSE scales.uhdrs_maximal_chorea_lle_12g
    END AS uhdrs_maximal_chorea_lle,
    CASE
        WHEN scales.uhdrs_gait_13 > 95 THEN NULL
        ELSE scales.uhdrs_gait_13
    END AS uhdrs_gait,
    CASE
        WHEN scales.uhdrs_tandem_walking_14 > 95 THEN NULL
        ELSE scales.uhdrs_tandem_walking_14
    END AS uhdrs_tandem_walking,
    CASE
        WHEN scales.uhdrs_tandem_retropulsion_pull_test_15 > 95 THEN NULL
        ELSE scales.uhdrs_tandem_retropulsion_pull_test_15
    END AS uhdrs_tandem_retropulsion_pull_test,
    CASE
        WHEN scales.uhdrs_diagnosis_confidence_level_17 > 95 THEN NULL
        ELSE scales.uhdrs_diagnosis_confidence_level_17
    END AS uhdrs_diagnosis_confidence_level

FROM rc_ataxia_pd_scales scales
ORDER BY
    subject_id,
    redcap_event_name
;
