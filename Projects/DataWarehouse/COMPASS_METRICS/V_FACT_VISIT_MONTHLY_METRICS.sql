CREATE OR REPLACE VIEW v_fact_visit_monthly_metrics AS
 SELECT
  a.network,
  a.visit_key,
  a.visit_id,
  p.patient_key,
  a.admission_dt_key,
  a.visit_number,
  f.facility_id,
  f.facility_name AS facility,
  rvt.visit_type_id,
  rvt.name AS visit_type,
  DECODE(med.payer_type, 'medicaid', 1, 0) medicaid_ind,
  DECODE(med.payer_type, 'medicare', 1, 0) medicare_ind,
  CAST(a.patient_id AS VARCHAR2(256)) AS patient_id,
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
  a.hypertension_ind,
  a.kidney_diseases_ind,
  a.smoker_ind,
  a.pregnancy_ind,
  a.pregnancy_onset_dt,
  a.flu_vaccine_ind,
  a.flu_vaccine_onset_dt,
  a.pna_vaccine_ind,
  a.pna_vaccine_onset_dt,
  a.bronchitis_ind,
  a.bronchitis_onset_dt,
  a.tabacco_scr_diag_ind,
  a.tabacco_scr_diag_onset_dt,
  a.nephropathy_screen_ind,
  a.retinal_dil_eye_exam_ind,
  a.tabacco_screen_proc_ind,
  a.a1c_final_calc_value,
  a.gluc_final_calc_value,
  a.ldl_final_calc_value,
  a.bp_final_calc_value,
  a.bp_final_calc_systolic,
  a.bp_final_calc_diastolic --, SOURCE, LOAD_DT
  
 FROM
  fact_visit_metric_results a
  -- JOIN fact_patient_metric_diag b ON a.network = b.network AND a.patient_id = b.patient_id
  JOIN dim_hc_facilities f ON f.facility_key = a.facility_key
  JOIN fact_visits v ON v.network = a.network AND v.visit_id = a.visit_id
  JOIN ref_visit_types rvt ON rvt.visit_type_id = a.final_visit_type_id
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
  a.admission_dt >= DATE '2014-01-01' AND a.admission_dt < TRUNC(ADD_MONTHS(SYSDATE, -1), 'MONTH');