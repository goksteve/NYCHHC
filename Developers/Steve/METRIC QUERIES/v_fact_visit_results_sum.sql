DROP VIEW V_FACT_VISITS_RESULTS_SUM;

CREATE OR REPLACE FORCE VIEW v_fact_visits_results_sum
 AS
 SELECT
  v.network,
  v.visit_id,
  v.patient_key,
  v.facility_key,
  v.admission_dt_key,
  v.discharge_dt_key,
  v.patient_id,
  v.admission_dt,
  v.discharge_dt,
  v.patient_age_at_admission,
  r.a1c_final_orig_value,
  r.a1c_final_calc_value,
  r.gluc_final_orig_value,
  r.gluc_final_calc_value,
  r.ldl_final_orig_value,
  r.ldl_final_calc_value,
  r.bp_final_orig_value,
  r.bp_calc_systolic,
  r.bp_calc_diastolic
 FROM
  fact_visits v JOIN fact_visit_metric_results r ON r.visit_id = v.visit_id AND r.network = v.network;


GRANT SELECT ON V_FACT_VISITS_RESULTS_SUM TO PUBLIC;
