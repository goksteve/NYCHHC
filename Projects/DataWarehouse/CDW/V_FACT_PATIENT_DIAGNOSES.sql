CREATE OR REPLACE VIEW V_FACT_PATIENT_DIAGNOSES
as
SELECT /*+ PARALLEL(32) */
			a.network,
			 p.patient_key,
			 a.problem_number,
			 DECODE(coding_scheme_id, 5, 'ICD-9', 'ICD-10') AS diag_coding_scheme,
			 a.code AS diag_code,
			 b.problem_type,
			 b.problem_description AS problem_comments,
			 b.provisional_flag,
			 b.onset_date,
			 b.stop_date AS end_date,
			 b.last_edit_time,
			 b.emp_provider_id,
			 b.status_id,
			 c.name AS problem_status,
			 b.primary_problem,
			 b.medical_problem_flag,
			 b.problem_list_type_id,
			 b.problem_severity_id
	  FROM problem_cmv a
			 JOIN problem b
				 ON a.patient_id = b.patient_id AND a.problem_number = b.problem_number AND a.network = b.network
			 JOIN dim_patients p ON p.patient_id = b.patient_id AND p.network = b.network AND current_flag = 1
			 LEFT JOIN problem_status c ON b.status_id = c.status_id AND b.network = c.network
	 WHERE	  1 = 1
			 AND a.coding_scheme_id IN (5, 10)
			 -- status of diagnosis must be "active".
			 AND b.status_id IN (0,
										6,
										7,
										8);
										