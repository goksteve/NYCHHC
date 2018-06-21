CREATE OR REPLACE VIEW cdw.v_compass_monthly_visits
AS
SELECT --+ parallel(32)
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
  (TRUNC (a.discharge_dt) - TRUNC (a.admission_dt)) los,
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
  a.bp_final_calc_diastolic,
  CASE WHEN r.visit_id IS NOT NULL THEN 1 ELSE 0 END
  AS readmission_ind,
  NVL(s.soarian_medicaid_flag, 0) AS soarian_medicaid_flag,
  NVL(s.soarian_medicare_flag, 0) AS soarian_medicare_flag,
  NVL(s.insured_flag, 0) AS insured_flag,
  NVL(s.soarian_payer, 'N/A') soarian_payer,
  NVL(s.soarian_payer_group, 'N/A') AS soarian_payer_group,
  CASE 
    WHEN pcp.pcp_visit_id IS NOT NULL 
    THEN 1 ELSE 0 
  END AS pcp_ind,
  CASE
    WHEN (pcp.pcp_attending_provider IS NOT NULL OR pcp.pcp_attending_provider_id IS NOT NULL)
    THEN 1
    ELSE 0
  END AS pcp_prov_ind,
  CASE
    WHEN (pcp.pcp_resident_emp_provider IS NOT NULL OR pcp.pcp_resident_provider_id IS NOT NULL)
    THEN 1
    ELSE 0
  END AS pcp_alt_prov_ind,
  pcp_clinic_code,
  pcp.specialty,
  pcp.service,
  pcp.service_type
FROM fact_visit_metrics a
LEFT JOIN pt008.readmission_details r
  ON a.source = DECODE (r.epic_flag,  'Y', 'EPIC',  'N', 'QCPR')
 AND a.network = r.network
 AND a.visit_id = r.visit_id
LEFT JOIN sorian_visit_map_final s
  ON a.visit_id = s.visit_id AND a.facility = s.facility_name
LEFT JOIN pcp_visits_all pcp
  ON a.network = pcp.network
 AND a.visit_id = pcp.pcp_visit_id
 AND a.source = pcp.source
UNION ALL
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
  (TRUNC(b.discharge_dt) - TRUNC (b.admission_dt)) los,
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
  b.bp_final_calc_diastolic,
  CASE 
    WHEN r.visit_id IS NOT NULL 
    THEN 1 ELSE 0 
  END AS readmission_ind,
  NVL(s.soarian_medicaid_flag, 0) AS soarian_medicaid_flag,
  NVL(s.soarian_medicare_flag, 0) AS soarian_medicare_flag,
  NVL(s.insured_flag, 0) AS insured_flag,
  NVL(s.soarian_payer, 'N/A') soarian_payer,
  NVL(s.soarian_payer_group, 'N/A') AS soarian_payer_group,
  CASE 
    WHEN pcp.pcp_visit_id IS NOT NULL 
    THEN 1 ELSE 0 
  END AS pcp_ind,
  CASE
    WHEN (pcp.pcp_attending_provider IS NOT NULL OR pcp.pcp_attending_provider_id IS NOT NULL)
    THEN 1
    ELSE 0
  END AS pcp_prov_ind,
  CASE
    WHEN (pcp.pcp_resident_emp_provider IS NOT NULL OR pcp.pcp_resident_provider_id IS NOT NULL)
    THEN 1
    ELSE 0
  END AS pcp_alt_prov_ind,
  pcp_clinic_code,
  pcp.specialty,
  pcp.service,
  pcp.service_type
FROM fact_visit_monthly_metrics b
LEFT JOIN pt008.readmission_details r
  ON b.network = r.network AND b.visit_id = r.visit_id
LEFT JOIN sorian_visit_map_final s
  ON b.visit_id = s.visit_id AND b.facility = s.facility_name
LEFT JOIN pcp_visits_all pcp
  ON b.network = pcp.network AND b.visit_id = pcp.pcp_visit_id
WHERE b.admission_dt < TRUNC (SYSDATE, 'MONTH');