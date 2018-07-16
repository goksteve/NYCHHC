CREATE OR REPLACE VIEW cdw.v_compass_monthly_visits
AS
SELECT --+ parallel(32)
 -- 12-Jul-2018, GK: Added visit key and reordered columns to match the compass_monthly_visits table. 
 -- 29-Jun-2018, Uma: modified sorian logic 
 -- 29-Jun-2018, Gk: Added Tobbaco, PNA, Flu fields
  a.network,
  a.visit_key,
  a.visit_id,
  a.patient_id,
  a.facility,
  a.admission_dt_key,
  a.admission_dt,
  a.discharge_dt,
  a.visit_type,
  a.mrn,
  a.patient_name,
  a.sex,
  a.birthdate,
  a.patient_age_at_admission,
  a.medicaid_ind,
  a.medicare_ind,
  s.soarian_medicaid_flag,
  s.soarian_medicare_flag,
  s.insured_flag,
  s.soarian_payer,
  s.soarian_payer_group,
  CASE     
    WHEN pcp.pcp_visit_id IS NOT NULL 
    THEN 1 
    ELSE 0 
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
  pcp.service_type,
  (TRUNC (discharge_dt) - TRUNC (admission_dt)) los,
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
  a.tabacco_scr_diag_ind,
  a.tabacco_scr_diag_onset_dt,
  a.tabacco_screen_proc_ind,
  a.flu_vaccine_ind,
  a.flu_vaccine_onset_dt,
  a.pna_vaccine_ind,
  a.pna_vaccine_onset_dt,  
  a.a1c_final_calc_value,
  a.gluc_final_calc_value,
  a.ldl_final_calc_value,
  a.bp_final_calc_value,
  a.bp_final_calc_systolic,
  a.bp_final_calc_diastolic,
  CASE     
    WHEN r.visit_id IS NOT NULL 
    THEN 1  
    ELSE 0 
  END AS readmission_ind
FROM fact_visit_metrics a
LEFT JOIN pt008.readmission_details r
  ON a.source = DECODE (r.epic_flag,  'Y', 'EPIC',  'N', 'QCPR')
 AND a.network = r.network
 AND a.visit_id = r.visit_id
LEFT JOIN 
(
  SELECT 
    DISTINCT visit_id, network,DECODE(SOURCE,'EPIC','EPIC','CDW','QCPR') AS source,
    CASE 
      WHEN medicaid_flag= 'Y' 
      THEN 1 
      ELSE 0 
    END AS soarian_medicaid_flag,
    CASE 
      WHEN medicare_flag ='Y' 
      THEN 1 
      ELSE  0 
    END AS soarian_medicare_flag,
    CASE 
      WHEN UNINSURED_FLAG ='Y' 
      THEN 0 
      ELSE 1 
    END AS insured_flag,
    NVL(primpyr, 'N/A') AS soarian_payer,
    NVL(primpyrrptgrp, 'N/A') AS soarian_payer_group
  FROM pt008.xx_cdw_soarian_pyr_tbl
) s
  ON a.visit_id = s.visit_id 
 AND a.NETWORK = s.NETWORK 
 AND a.source=s.source
LEFT JOIN pcp_visits_all pcp
  ON a.network = pcp.network
 AND a.visit_id = pcp.pcp_visit_id
 AND a.source = pcp.source
 
UNION ALL

SELECT 
  b.network,
  b.visit_key,
  b.visit_id,
  CAST(b.patient_id AS VARCHAR2 (256)) AS patient_id,
  b.facility,
  b.admission_dt_key,
  b.admission_dt,
  b.discharge_dt,
  b.visit_type,
  b.mrn,
  b.patient_name,
  b.sex,
  b.birthdate,
  b.patient_age_at_admission,
  b.medicaid_ind,
  b.medicare_ind,
  s.soarian_medicaid_flag,
  s.soarian_medicare_flag,
  s.insured_flag,
  s.soarian_payer,
  s.soarian_payer_group,
  CASE    
    WHEN pcp.pcp_visit_id IS NOT NULL    
    THEN 1 
    ELSE 0  
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
  pcp.service_type,
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
  b.tabacco_scr_diag_ind,
  b.tabacco_scr_diag_onset_dt,
  b.tabacco_screen_proc_ind,
  b.flu_vaccine_ind,
  b.flu_vaccine_onset_dt,
  b.pna_vaccine_ind,
  b.pna_vaccine_onset_dt,    
  b.a1c_final_calc_value,
  b.gluc_final_calc_value,
  b.ldl_final_calc_value,
  b.bp_final_calc_value,
  b.bp_final_calc_systolic,
  b.bp_final_calc_diastolic,
  CASE 
    WHEN r.visit_id IS NOT NULL    
    THEN 1 
    ELSE 0  
  END AS readmission_ind
FROM fact_visit_monthly_metrics b
LEFT JOIN pt008.readmission_details r
  ON b.network = r.network AND b.visit_id = r.visit_id
LEFT JOIN 
(
  SELECT
    DISTINCT visit_id, network,DECODE (source,'EPIC', 'EPIC','CDW','QCPR') AS source,
    CASE 
      WHEN medicaid_flag= 'Y' 
      THEN 1 
      ELSE 0 
    END AS soarian_medicaid_flag,
    CASE 
      WHEN medicare_flag ='Y' 
      THEN 1 
      ELSE 0 
    END AS soarian_medicare_flag,
    CASE 
      WHEN UNINSURED_FLAG ='Y' 
      THEN 0 
      ELSE 1 
    END AS insured_flag,
    NVL (PRIMPYR, 'N/A') soarian_payer,
    NVL (PRIMPYRRPTGRP, 'N/A') AS soarian_payer_group
  FROM pt008.xx_cdw_soarian_pyr_tbl
) s
  ON b.visit_id = s.visit_id 
 AND b.NETWORK = s.NETWORK 
 AND b.source=s.source
LEFT JOIN pcp_visits_all pcp
  ON b.network = pcp.network AND b.visit_id = pcp.pcp_visit_id
WHERE b.admission_dt < TRUNC (SYSDATE, 'MONTH');