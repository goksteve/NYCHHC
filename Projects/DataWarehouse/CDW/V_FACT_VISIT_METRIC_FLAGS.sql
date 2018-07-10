CREATE OR REPLACE VIEW v_fact_visit_metric_flags AS


--2018 - JUNE -22 created
  WITH crit_metric AS
   (
    SELECT --+ materialize 
    network, criterion_id, VALUE
    FROM meta_conditions
    WHERE criterion_id IN (4,10,23,13,66,68)   ), -- A1C, LDL, Glucose,  BP, Neph, eye eaxm



rslt
As
  (SELECT --+ materialize
      r.network,
      r.visit_id,
      r.patient_key,
      r.patient_id,
      result_dt,
      result_value,
      c.criterion_id,
      ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY result_dt DESC) rnum
      FROM    crit_metric c
      JOIN fact_results r  ON r.data_element_id = c.VALUE AND r.network = c.network
      AND r.event_status_id IN (6, 11)   AND r.network =  SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
      WHERE
      TRIM(r.result_value) IS NOT NULL
   UNION ALL
           SELECT  r.network, r.visit_id,   
                   patient_key, patient_id,   result_dt,
            result_value, 98 as criterion_id,
             ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id ORDER BY result_dt DESC) rnum
            FROM    ref_proc_descriptions f 
                  JOIN fact_results r ON f.proc_key = r.proc_key
            WHERE proc_type_id = 98 AND in_ind = 'I'
                  AND r.network =  SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
                  AND TRIM(r.result_value) IS NOT NULL
   ),

calc_result AS
(
SELECT --+ materialize
 v.network,
 v.visit_id,
 v.visit_key,
 v.patient_key,
 v.facility_key,
 v.admission_dt_key,
 v.discharge_dt_key,
 v.visit_number,
 v.patient_id,
 v.admission_dt,
 v.discharge_dt,
 v.patient_age_at_admission,
 v.first_payer_key,
 v.initial_visit_type_id,
 v.final_visit_type_id,
 p.asthma_ind,
 p.bh_ind,
 p.breast_cancer_ind,
 p.diabetes_ind,
 p.heart_failure_ind,
 p.hypertension_ind,
 p.kidney_diseases_ind,
 p.smoker_ind,
 p.pregnancy_ind,
 pregnancy_onset_dt,
 p.flu_vaccine_ind,
 p.flu_vaccine_onset_dt, 
 p.pna_vaccine_ind, 
 p.pna_vaccine_onset_dt,
 p.bronchitis_ind,	
 p.bronchitis_onset_dt,
 p.tabacco_scr_diag_ind,       
 p.tabacco_scr_diag_onset_dt, 
 TRUNC(q.result_dt) AS result_dt,
 q.criterion_id,
 q.result_value,
 1 as flag
  FROM
  fact_visits v 
  LEFT JOIN  fact_patient_metric_diag p ON p.patient_id = v.patient_id AND p.network = v.network  
  LEFT  JOIN rslt q ON q.visit_id = v.visit_id AND q.network = v.network AND q.rnum = 1
  WHERE  v.network =   SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
  AND Admission_dt >= DATE '2014-01-01'
),


final_calc_tb
AS
(
    SELECT --+ materialize
    network,
    visit_id,
    patient_key,
    visit_key,
    facility_key,
    admission_dt_key,
    discharge_dt_key,
    visit_number,
    patient_id,
    admission_dt,
    discharge_dt,
    patient_age_at_admission,
    first_payer_key,
    initial_visit_type_id,
    final_visit_type_id,
    asthma_ind,
    bh_ind,
    breast_cancer_ind,
    diabetes_ind,
    heart_failure_ind,
    hypertension_ind,
    kidney_diseases_ind,
    smoker_ind,
    pregnancy_ind,
    pregnancy_onset_dt,
    flu_vaccine_ind,
    flu_vaccine_onset_dt,
    pna_vaccine_ind, 
    pna_vaccine_onset_dt,
    bronchitis_ind,	
    bronchitis_onset_dt,
    tabacco_scr_diag_ind,       
    tabacco_scr_diag_onset_dt, 
	  neph_final_result_dt,
    neph_final_orig_value,
    neph_flag,
    retinal_final_result_dt,
    retinal_final_orig_value,
    retinal_flag,
    tabacco_final_orig_value, 
    tabacco_flag,
    a1c_final_result_dt,
    a1c_final_orig_value,
    a1c_flag,
    gluc_final_result_dt,
    gluc_final_orig_value,
    gluc_flag,
    ldl_final_result_dt,
    ldl_final_orig_value,
    ldl_flag,
    bp_final_result_dt,
    bp_final_orig_value,
    bp_flag
FROM
 calc_result
  PIVOT
   ( MAX(result_dt) AS final_result_dt,
     MAX(result_value) AS final_orig_value, 
     MAX(flag) as flag
     FOR criterion_id
   IN (4 AS a1c, 23 AS gluc, 10 AS ldl, 13 AS bp, 66 as neph, 68 as retinal , 98 as tabacco ))
)

 SELECT --+  PARALLEL (48)
  a.network,
  a.visit_id,
  a.visit_key,
  a.patient_key,
  a.facility_key,
  a.admission_dt_key,
  a.discharge_dt_key,
  a.visit_number,
  a.patient_id,
  a.admission_dt,
  a.discharge_dt,
  a.patient_age_at_admission,
  a.first_payer_key,
  a.initial_visit_type_id,
  a.final_visit_type_id,
  NVL(a.asthma_ind, 0) asthma_ind,
  NVL(a.bh_ind, 0) bh_ind,
  NVL(a.breast_cancer_ind, 0) breast_cancer_ind,
  NVL(a.diabetes_ind, 0) diabetes_ind,
  NVL(a.heart_failure_ind, 0) heart_failure_ind,
  NVL(a.hypertension_ind, 0) hypertansion_ind,
  NVL(a.kidney_diseases_ind, 0) kidney_diseases_ind,
  NVL(smoker_ind, 0) AS smoker_ind,
  NVL(pregnancy_ind, 0) AS pregnancy_ind,
  pregnancy_onset_dt,
  NVL(flu_vaccine_ind, 0) AS flu_vaccine_ind,
  flu_vaccine_onset_dt,
  NVL(pna_vaccine_ind, 0) AS pna_vaccine_ind,
  pna_vaccine_onset_dt,
  NVL(bronchitis_ind, 0) AS bronchitis_ind,
  bronchitis_onset_dt,
  NVL(tabacco_scr_diag_ind,0) AS tabacco_scr_diag_ind,     
  tabacco_scr_diag_onset_dt, 
  NVL(a.neph_flag, 0) AS nephropathy_screen_ind,
  NVL(a.retinal_flag, 0) AS retinal_dil_eye_exam_ind,
  NVL( a.tabacco_flag, 0)AS tabacco_screen_proc_ind, 
  a.a1c_final_result_dt,
  a1c_final_orig_value,
  NVL(a1c_flag,0) AS a1c_flag,
  a.gluc_final_result_dt,
  gluc_final_orig_value,
  NVL(gluc_flag,0) AS gluc_flag,
  a.ldl_final_result_dt,
  ldl_final_orig_value,
  NVL(ldl_flag,0) ldl_flag,
  bp_final_result_dt,
  bp_final_orig_value,
  NVL(bp_flag,0) bp_flag
FROM
 final_calc_tb a
WHERE
 NVL(a.asthma_ind, 0) <> 0
 OR NVL(a.bh_ind, 0) <> 0
 OR NVL(a.breast_cancer_ind, 0) <> 0
 OR NVL(a.diabetes_ind, 0) <> 0
 OR NVL(a.heart_failure_ind, 0) <> 0
 OR NVL(a.hypertension_ind, 0) <> 0
 OR NVL(a.kidney_diseases_ind, 0) <> 0
 OR NVL(smoker_ind, 0) <> 0
 OR NVL(pregnancy_ind, 0) <> 0
 OR NVL(flu_vaccine_ind, 0) <> 0
 OR NVL(pna_vaccine_ind, 0) <> 0
 OR NVL(bronchitis_ind, 0) <> 0
 OR  NVL(tabacco_scr_diag_ind,0) <> 0
 OR NVL(a.neph_flag, 0) <> 0
 OR NVL(a.retinal_flag, 0) <> 0
 OR NVL(a.tabacco_flag, 0) <> 0
 OR NVL(a.a1c_flag,0) <> 0
 OR NVL(a.gluc_flag,0)<> 0
 OR NVL(a.ldl_flag,0) <> 0
 OR NVL(a.bp_flag,0) <>0