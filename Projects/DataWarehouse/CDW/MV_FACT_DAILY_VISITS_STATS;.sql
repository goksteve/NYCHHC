DROP MATERIALIZED VIEW mv_fact_daily_visits_stats;

CREATE MATERIALIZED VIEW mv_fact_daily_visits_stats
 BUILD IMMEDIATE
 REFRESH
  COMPLETE
  START WITH TRUNC(SYSDATE) + 23 / 24
  NEXT (TRUNC(SYSDATE) + 1) + 23 / 24 AS
 SELECT
  a.network,
  a.visit_id,
  a.visit_number,
  a.facility_id,
  a.facility,
  a.visit_type_id,
  a.visit_type,
  a.medicaid_ind,
  CAST(a.patient_id AS VARCHAR2(256)) patient_id,
  a.mrn,
  a.pat_lname || ', ' || a.pat_fname AS patient_name,
  a.sex,
  TRUNC(a.birthdate) AS birthdate,
  a.age,
  TRUNC(a.admission_dt) AS admission_dt,
  TRUNC(a.discharge_dt) AS discharge_dt,
  NVL(b.asthma_ind, 0) AS asthma_ind,
  NVL(b.bh_ind, 0) AS bh_ind,
  NVL(b.breast_cancer_ind, 0) AS breast_cancer_ind,
  NVL(b.diabetes_ind, 0) AS diabetes_ind,
  NVL(b.heart_failure_ind, 0) AS heart_failure_ind,
  NVL(b.hypertansion_ind, 0) AS hypertansion_ind,
  NVL(b.kidney_diseases_ind, 0) AS kidney_diseases_ind,
  NVL(b.pregnancy_ind, 0) AS pregnancy_ind,
  TRUNC(b.pregnancy_onset_dt) pregnancy_onset_dt,
  'QCPR' AS source
 FROM
  fact_daily_visits_stats a
  LEFT JOIN fact_patient_metric_diag b ON b.network = a.network AND a.patient_id = b.patient_id
 WHERE
  admission_dt < TRUNC(SYSDATE)
 UNION
 SELECT
  network,
  visit_id,
  NULL visit_number,
  facility_key,
  facility_name,
  NULL visit_type_id,
  visit_type,
  NULL medicaid_ind,
  patient_id,
  mrn,
  patient_name,
  sex,
  TRUNC(CAST(birth_date AS DATE)) birth_date,
  age,
  TRUNC(CAST(admission_dt AS DATE)) admission_dt,
  TRUNC(CAST(discharge_dt AS DATE)) discharge_dt,
  asthma_ind,
  bh_ind,
  breast_cancer_ind,
  diabetes_ind,
  heart_failure_ind,
  hypertansion_ind,
  kidney_diseases_ind,
  pregnancy_ind,
  TRUNC(pregnancy_onset_dt),
  'EPIC' AS source
 FROM
  epic_clarity.epic_fact_daily_visits_stats;

GRANT SELECT ON mv_fact_daily_visits_stats TO PUBLIC