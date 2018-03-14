CREATE OR REPLACE VIEW v_fact_visit_diagnoses AS
SELECT
 -- 9-Mar-2018, SG: created
  ap.network,
  ap.visit_id,
  ap.problem_number AS problem_nbr,
  pid.code AS icd_code,
  DECODE(pid.coding_scheme_id, '5', 'ICD-9', 'ICD-10') coding_scheme,
  pp.patient_key,
  pp.patient_id,
  vv.facility_key,
  ap.diagnosis_dt,
  TO_NUMBER(TO_CHAR(ap.diagnosis_dt, 'YYYYMMDDHH24MISS')) diagnosis_dt_key, 
  ap.is_primary_problem,
  prob.problem_description AS problem_comments,
  prob.status_id AS problem_status_id,
  'QCPR' AS source
FROM
(
  SELECT
    ap.network, ap.visit_id, ap.problem_number,
    MAX(CASE WHEN rf.name IN ('ICD-10 CM', 'ICD-9 Diagnosis', 'ICD-9 Procedure') THEN 'Y' ELSE 'N' END) AS is_primary_problem,
    MAX(e.date_time) AS diagnosis_dt
  FROM active_problem ap
  JOIN event e ON e.network = ap.network AND e.visit_id = ap.visit_id AND e.event_id = ap.event_id
  JOIN result_field rf
    ON rf.data_element_id = ap.data_element_id AND ap.network = rf.network
  GROUP BY ap.network, ap.visit_id, ap.problem_number
) ap
JOIN fact_visits vv
  ON vv.visit_id = ap.visit_id AND vv.network = ap.network
JOIN dim_patients pp
  ON pp.network = vv.network AND pp.patient_id = vv.patient_id
 AND pp.effective_from <= ap.diagnosis_dt AND pp.effective_to > ap.diagnosis_dt 
JOIN problem prob
  ON prob.problem_number = ap.problem_number AND prob.patient_id = vv.patient_id AND prob.network = ap.network
 AND prob.status_id IN (0, 6, 7, 8) 
JOIN problem_cmv pid
  ON pid.problem_number = prob.problem_number AND pid.patient_id = prob.patient_id AND pid.network = prob.network
 AND pid.coding_scheme_id IN ('5','10');
