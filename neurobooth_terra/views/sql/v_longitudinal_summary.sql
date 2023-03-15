CREATE OR REPLACE VIEW v_longitudinal_summary AS
WITH latest_diagnosis AS (
	SELECT DISTINCT ON (subject_id)
		subject_id,
		primary_diagnosis,
		other_primary_diagnosis,
		other_ataxia,
		other_neuropathy,
		other_dementia,
		secondary_diagnosis,
		diagnosis_notes
	FROM rc_clinical
	ORDER BY
		subject_id,
		end_time_clinical DESC NULLS LAST
), visit_summary AS (
	SELECT
		visit.subject_id,
		COUNT(visit.subject_id) AS num_visits,
		MIN(visit.neurobooth_visit_dates) AS first_visit,
		MAX(visit.neurobooth_visit_dates) AS last_visit,
		MAX(visit.neurobooth_visit_dates) - MIN(visit.neurobooth_visit_dates) AS total_days
	FROM rc_visit_dates visit
	JOIN rc_participant_and_consent_information subj_info
		ON visit.subject_id = subj_info.subject_id
	WHERE subj_info.test_subject_boolean = FALSE
	GROUP BY
		visit.subject_id
)
SELECT
	visit_summary.subject_id,
	visit_summary.num_visits,
	visit_summary.first_visit,
	visit_summary.last_visit,
	visit_summary.total_days,
	latest_diagnosis.primary_diagnosis,
	latest_diagnosis.other_primary_diagnosis,
	latest_diagnosis.other_ataxia,
	latest_diagnosis.other_neuropathy,
	latest_diagnosis.other_dementia,
	latest_diagnosis.secondary_diagnosis,
	latest_diagnosis.diagnosis_notes
FROM visit_summary
LEFT JOIN latest_diagnosis
	ON visit_summary.subject_id = latest_diagnosis.subject_id
ORDER BY
	visit_summary.subject_id
;