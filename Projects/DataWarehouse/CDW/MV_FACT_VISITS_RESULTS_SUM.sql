CREATE MATERIALIZED VIEW mv_fact_visits_results_sum
 NOLOGGING
 REFRESH  ON DEMAND  COMPLETE 
AS
 SELECT /*+ PARALLEL (32) */
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
  r.glucose_final_orig_value,
  r.glucose_final_calc_value,
  r.ldl_final_orig_value,
  r.ldl_final_calc_value,
  r.bp_final_orig_value,
  r.bp_calc_systolic,
  r.bp_calc_diastolic
 FROM
  fact_visits v JOIN fact_visit_metric_results r ON r.visit_id = v.visit_id AND r.network = v.network;

CREATE INDEX uk_mv_fact_visit_result_sum
 ON mv_fact_visits_results_sum(network, visit_id, patient_id);

GRANT SELECT ON mv_fact_visits_results_sum TO PUBLIC;