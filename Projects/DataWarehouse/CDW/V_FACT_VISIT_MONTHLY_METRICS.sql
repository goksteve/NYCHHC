CREATE OR REPLACE VIEW v_fact_visit_monthly_metrics
AS
SELECT
  a.network,
  --  a.visit_key,  remove visit_key from daily
  a.visit_id,
  --  patient_key, remove patient_key from daily
  a.admission_dt_key,
  --  a.visit_number,  remove from daily
  f.facility_name AS facility,
  --  v.visit_type_id,  remove from daily
  rvt.name AS visit_type,
  cast(a.PATIENT_ID as varchar2(256)) as patient_id,
  NVL(rmrn.second_mrn, p.medical_record_number) mrn,
  p.name AS patient_name,
  p.sex,
  p.race_desc AS race,
  p.birthdate,
  a.patient_age_at_admission,
  a.admission_dt,
  a.discharge_dt,
  a.asthma_ind,
  a.bh_ind,
  a.breast_cancer_ind,
  a.diabetes_ind,
  a.heart_failure_ind,
  hypertension_ind,
  a.kidney_diseases_ind,
  smoker_ind,
  a.pregnancy_ind,
  a.pregnancy_onset_dt,
  nephropathy_screen_ind,
  retinal_dil_eye_exam_ind,
  a1c_final_calc_value,
  gluc_final_calc_value,
  ldl_final_calc_value,
  bp_final_calc_value,
  bp_final_calc_systolic,
  bp_final_calc_diastolic, --, SOURCE, LOAD_DT
  DECODE(med.payer_type, 'medicaid', 1, 0) medicaid_ind,
  DECODE(med.payer_type, 'medicare', 1, 0) medicare_ind
 FROM
  fact_visit_metric_results a
  JOIN fact_patient_metric_diag b ON a.network = b.network AND a.patient_id = b.patient_id
  JOIN dim_hc_facilities f ON f.facility_key = a.facility_key
  JOIN fact_visits v ON v.network = a.network AND v.visit_id = a.visit_id
  JOIN ref_visit_types rvt ON rvt.visit_type_id = v.final_visit_type_id
  JOIN dim_patients p ON p.network = v.network AND p.patient_id = a.patient_id AND p.current_flag = 1
  LEFT JOIN ref_patient_secondary_mrn rmrn
   ON rmrn.network = a.network AND rmrn.facility_key = a.facility_key AND rmrn.patient_id = a.patient_id
  LEFT JOIN
  (SELECT
    p.payer_key,
    CASE
     WHEN (TRIM(UPPER(p.payer_name)) LIKE '%MEDICAID%' OR TRIM(UPPER(p.payer_name)) LIKE 'MCAID%') THEN
      'medicaid'
     WHEN TRIM(UPPER(p.payer_name)) LIKE '%MEDICARE%' THEN
      'medicare'
    END
     AS payer_type
   FROM
    dim_payers p
   WHERE
    (TRIM(UPPER(p.payer_name)) LIKE '%MEDICAID%'
     OR TRIM(UPPER(p.payer_name)) LIKE 'MCAID%'
     OR TRIM(UPPER(p.payer_name)) LIKE '%MEDICARE%')) med
   ON med.payer_key = v.first_payer_key
 WHERE
 a.admission_dt >=  DATE '2017-01-01'
AND  a.admission_dt < TRUNC(ADD_MONTHS(SYSDATE, -1), 'MONTH');