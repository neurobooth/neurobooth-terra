CREATE OR REPLACE VIEW rc_visit_dates_clean AS
SELECT
    visit.subject_id,
    visit.redcap_event_name,
    (regexp_match(visit.redcap_event_name, 'v(\d+)_arm_\d+'))[1]::int AS redcap_sequence_num,
	(regexp_match(visit.redcap_event_name, 'v\d+_arm_(\d+)'))[1]::int AS redcap_study_arm,
	CASE
        WHEN visit.visit_dates_complete = 2 THEN TRUE
        ELSE FALSE
    END AS visit_dates_complete,
	visit.neurobooth_visit_dates,
	visit.neurobooth_visit_time
FROM rc_visit_dates visit
ORDER BY
    subject_id,
    redcap_event_name