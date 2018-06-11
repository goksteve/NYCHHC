CREATE OR REPLACE FORCE VIEW cdw.v_compass_monthly_visits
AS
SELECT 
  a.network,
  a.visit_id,
  a.admission_dt_key,
  a.facility,
  a.visit_type,
  a.medicaid_ind,
  a.medicare_ind,
  a.patient_id,
  a.mrn,
  a.patient_name,
  a.sex,
  a.birthdate,
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
  a.nephropathy_screen_ind,
  a.retinal_dil_eye_exam_ind,
  a.a1c_final_calc_value,
  a.gluc_final_calc_value,
  a.ldl_final_calc_value,
  a.bp_final_calc_value,
  a.bp_final_calc_systolic,
  a.bp_final_calc_diastolic
FROM fact_visit_metrics a

UNION

SELECT 
  b.network,
  b.visit_id,
  b.admission_dt_key,
  b.facility,
  b.visit_type,
  b.medicaid_ind,
  b.medicare_ind,
  CAST(b.patient_id AS VARCHAR2 (256)) AS patient_id,
  b.mrn,
  b.patient_name,
  b.sex,
  b.birthdate,
  b.patient_age_at_admission,
  b.admission_dt,
  b.discharge_dt,
  b.asthma_ind,
  b.bh_ind,
  b.breast_cancer_ind,
  b.diabetes_ind,
  b.heart_failure_ind,
  b.hypertension_ind,
  b.kidney_diseases_ind,
  b.smoker_ind,
  b.pregnancy_ind,
  b.pregnancy_onset_dt,
  b.nephropathy_screen_ind,
  b.retinal_dil_eye_exam_ind,
  b.a1c_final_calc_value,
  b.gluc_final_calc_value,
  b.ldl_final_calc_value,
  b.bp_final_calc_value,
  b.bp_final_calc_systolic,
  b.bp_final_calc_diastolic
FROM fact_visit_monthly_metrics b
WHERE b.admission_dt < TRUNC (SYSDATE, 'MONTH');

CREATE OR REPLACE PUBLIC SYNONYM v_compass_monthly_visits FOR cdw.v_compass_monthly_visits;

GRANT SELECT ON cdw.v_compass_monthly_visits TO PUBLIC;    