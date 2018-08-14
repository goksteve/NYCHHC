CREATE OR REPLACE VIEW v_fact_patient_diagnoses AS
SELECT
 -- 20-Mar-2018, GK: modified dim_patients join condition to use current_flag insted of effective_from,effective_to, Data issue(patient_id=157688 and network='QHN' and problem_number =1)
 -- 09-Mar-2018, OK: added column PATIENT_ID
 -- 08-Mar-2018, SG: created
  a.network,
  p.patient_key,
  p.patient_id,
  a.problem_number,
  DECODE(coding_scheme_id, 5, 'ICD-9', 'ICD-10') AS diag_coding_scheme,
  a.code AS diag_code,
  b.problem_type,
  b.problem_description AS problem_comments,
  b.provisional_flag,
  b.onset_date,
  b.stop_date AS end_date,
  CASE WHEN ind = 1 THEN  ADD_MONTHS( onset_date, 9)  END  AS  calc_excl_end_date,
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
JOIN dim_patients p
  ON p.patient_id = b.patient_id AND p.network = b.network AND p.current_flag = 1
LEFT JOIN problem_status c
  ON b.status_id = c.status_id AND b.network = c.network
left join (SELECT  --- create calculate endd dates for pregnant women ********
       DISTINCT  1 as ind , value
FROM
 meta_conditions a JOIN meta_criteria b ON a.criterion_id = b.criterion_id AND b.criterion_cd LIKE 'DIAG%'
WHERE
 include_exclude_ind = 'E'
 AND (UPPER(a.value_description)     NOT LIKE '%PREGNAN%'
      AND UPPER(a.value_description) NOT LIKE '%GESTATION%'
      AND UPPER(a.value_description) NOT LIKE '%MATERNAL%'
      AND UPPER(a.value_description) NOT LIKE '%LACTATION%'
      AND UPPER(a.value_description) NOT LIKE '%TRIMESTER%'
      AND UPPER(a.value_description) NOT LIKE '%PLACENT%'
      AND UPPER(a.value_description) NOT LIKE '%FETAL%'
      AND UPPER(a.value_description) NOT LIKE '%EYE%')
) calc on calc.value  =   a.code

WHERE a.coding_scheme_id IN (5, 10)
AND b.status_id IN (0, 6, 7, 8);