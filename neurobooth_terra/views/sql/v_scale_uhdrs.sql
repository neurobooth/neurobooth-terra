-- ================================================================================
-- Reflects the subset of rc_ataxia_pd_scales_clean related to UHDRS
-- Any "business logic" should likely be performed in the source view.
-- ================================================================================

CREATE OR REPLACE VIEW v_scale_uhdrs AS
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
    -- UHDRS
    -- ========================================
    scales.uhdrs_pursuit_horizontal,
    scales.uhdrs_pursuit_vertical,
    scales.uhdrs_saccade_initation_horizontal,
    scales.uhdrs_saccade_initation_vertical,
    scales.uhdrs_saccade_velocity_horizontal,
    scales.uhdrs_saccade_velocity_vertical,
    scales.uhdrs_dysarthria,
    scales.uhdrs_tongue_protrusion,
    scales.uhdrs_finger_taps_right,
    scales.uhdrs_finger_taps_left,
    scales.uhdrs_pronate_supinate_hands_right,
    scales.uhdrs_pronate_supinate_hands_left,
    scales.uhdrs_luria,
    scales.uhdrs_rigidity_arms_right,
    scales.uhdrs_rigidity_arms_left,
    scales.uhdrs_bradykinesia_body,
    scales.uhdrs_maximal_dystonia_trunk,
    scales.uhdrs_maximal_dystonia_rue,
    scales.uhdrs_maximal_dystonia_lue,
    scales.uhdrs_maximal_dystonia_rle,
    scales.uhdrs_maximal_dystonia_lle,
    scales.uhdrs_maximal_chorea_face,
    scales.uhdrs_maximal_chorea_bol,
    scales.uhdrs_maximal_chorea_trunk,
    scales.uhdrs_maximal_chorea_rue,
    scales.uhdrs_maximal_chorea_lue,
    scales.uhdrs_maximal_chorea_rle,
    scales.uhdrs_maximal_chorea_lle,
    scales.uhdrs_gait,
    scales.uhdrs_tandem_walking,
    scales.uhdrs_tandem_retropulsion_pull_test,
    scales.uhdrs_diagnosis_confidence_level

FROM rc_ataxia_pd_scales_clean scales
WHERE scales.uhdrs_performed_boolean = TRUE
