CREATE OR REPLACE VIEW v_fact_visit_diagnoses
AS
SELECT  
--- Created by SG 03-09-2018
DISTINCT ap.network,
  ap.visit_id,
  ap.problem_number AS problem_nbr,
  ap.event_id,
  pp.patient_key,
  pp.patient_id,
  vv.facility_key,
  ee.date_time AS event_date_time,
  'ICD10' diag_coding_scheme , --DECODE(pid.CODING_SCHEME_ID ,
  pid.code,
  CASE 	 WHEN rf.name IN ('ICD-10 CM', 'ICD-9 Diagnosis', 'ICD-9 Procedure')
  THEN 'Primary Diagnosis'
  ELSE rf.name
  END
  AS label,
  'ICD' AS TYPE,
  prob.problem_description AS problem_comments,
  prob.status_id
FROM
  active_problem ap
  JOIN  event ee	 ON ee.visit_id = ap.visit_id			AND ee.event_id = ap.event_id			AND ee.network = ap.network		
  JOIN  fact_visits vv ON vv.visit_id = ee.visit_id AND vv.network = ee.network
  JOIN  dim_patients  pp on pp.network = vv.network and pp.patient_id  =  vv.Patient_id AND pp.current_flag  = 1
  JOIN  result_field rf ON rf.data_element_id = ap.data_element_id AND ap.network = rf.network
  JOIN  problem prob 	 ON prob.problem_number = ap.problem_number 	AND prob.patient_id = ap.patient_id 			AND ap.network = prob.network
  JOIN  problem_cmv pid 	 ON pid.problem_number = prob.problem_number 			AND pid.patient_id = prob.patient_id 	AND pid.network = prob.network 			AND coding_scheme_id = '10'
WHERE ap.problem_number is not null 
;

COMMIT;