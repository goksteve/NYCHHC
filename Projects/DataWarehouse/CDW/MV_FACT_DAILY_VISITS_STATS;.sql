DROP MATERIALIZED VIEW mv_fact_daily_visits_stats;

CREATE MATERIALIZED VIEW mv_fact_daily_visits_stats
 BUILD IMMEDIATE
 REFRESH COMPLETE
  START WITH TRUNC(SYSDATE) + 23/ 24
  NEXT (TRUNC(SYSDATE) +1)+ 23 / 24 AS
 SELECT
  a.network,
  a.visit_id,
  a.visit_number,
  a.facility_id,
  a.facility,
  a.visit_type_id,
  a.visit_type,
  a.medicaid_ind,
  a.patient_id,
  a.mrn,
  a.pat_lname,
  a.pat_fname,
  a.sex,
  trunc(a.birthdate) as birthdate ,
  a.age,
  a.admission_dt,
  a.discharge_dt,
  NVL(b.asthma_ind,0) AS asthma_ind,
  NVL(b.bh_ind,0) AS bh_ind,
  NVL(b.breast_cancer_ind,0) AS breast_cancer_ind,
  NVL(b.diabetes_ind,0) AS diabetes_ind,
  NVL(b.heart_failure_ind,0) AS heart_failure_ind,
  NVL(b.hypertansion_ind,0) AS hypertansion_ind,
  NVL(b.kidney_diseases_ind,0) AS kidney_diseases_ind,
  NVL(b.pregnancy_ind,0) AS pregnancy_ind,
      b.pregnancy_onset_dt
 FROM
  fact_daily_visits_stats a
  LEFT JOIN fact_patient_metric_diag b ON b.network = a.network AND a.patient_id = b.patient_id
where  admission_dt < trunc(sysdate);
 
grant select on MV_FACT_DAILY_VISITS_STATS to public