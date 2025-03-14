CREATE OR REPLACE VIEW v_longitudinal_summary AS
WITH latest_diagnosis AS (
	SELECT DISTINCT ON (subject_id)
		subject_id,
		latest_primary_diagnosis,
		latest_primary_diagnosis_id,
		secondary_diagnosis,
		diagnosis_notes
	FROM rc_clinical_clean
	ORDER BY
		subject_id,
		date_enrolled DESC NULLS LAST
), latest_demographic AS (
	SELECT DISTINCT ON (subject_id)
		subject_id,
		age_first_contact,
		gender
	FROM rc_demographic_clean
	ORDER BY
		subject_id,
		end_time_demographic DESC NULLS LAST
), visit_summary AS (
	SELECT
		visit.subject_id,
		COUNT(visit.subject_id) AS num_visits,
		MIN(visit.neurobooth_visit_dates) AS first_visit,
		MAX(visit.neurobooth_visit_dates) AS last_visit,
		MAX(visit.neurobooth_visit_dates) - MIN(visit.neurobooth_visit_dates) AS total_days
	FROM rc_visit_dates visit
	JOIN rc_baseline_data subj_info
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
	latest_demographic.age_first_contact,
	latest_demographic.gender,
	latest_diagnosis.latest_primary_diagnosis,
	latest_diagnosis.latest_primary_diagnosis_id,
	latest_diagnosis.secondary_diagnosis,
	latest_diagnosis.diagnosis_notes
FROM visit_summary
LEFT JOIN latest_diagnosis
	ON visit_summary.subject_id = latest_diagnosis.subject_id
LEFT JOIN latest_demographic
	ON visit_summary.subject_id = latest_demographic.subject_id
ORDER BY
	visit_summary.subject_id
;