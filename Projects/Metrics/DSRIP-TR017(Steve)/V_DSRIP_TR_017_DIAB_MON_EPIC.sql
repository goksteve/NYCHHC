CREATE OR REPLACE VIEW v_dsrip_tr_017_diab_mon_epic AS

 WITH max_prt_dt AS (
  SELECT MAX(NVL(report_dt, to_date('01/01/2018', 'MM/DD.YYYY'))) rpt_dt FROM dsrip_tr_017_diab_mon_epic
)
 SELECT
  parent_location_name,
  location_id,
  location_name,
  mrn_empi,
  location_mrn,
  pat_name,
  birth_date,
  age_years,
  address_line_1,
  address_line_2,
  address_state,
  address_zip,
  home_phone,
  pcp_general_name,
  contactdate,
  hospital_discharge_date,
  encounter_type,
  inspayor1,
  inspayor2,
  inspayor3,
  icd10_codes,
  diabetic_medication_name,
  hemoglobin_order_time,
  hemoglobin_result_time,
  hemoglobin_result_value,
  ldl_c_order_time,
  ldl_c__result_time,
  ldl_c_result_value,
  numr_flag_ldl_c_and_hemo_test,
  last_primary_care_visit_dt,
  last_primary_care_visit_dep_nm,
  last_behav_hlth_visit_date,
  last_behav_hlth_visit_dep_nm,
  last_behav_hlth_visit_prov_nm,
  source,
  epic_flag,
  TRUNC(etl_load_date, 'MONTH') AS report_dt,
  etl_load_date
 FROM
  epic_clarity.dsrip_diab_mon_epic

 WHERE
   TRUNC(etl_load_date) > TRUNC(SYSDATE, 'MONTH')
  AND TRUNC(etl_load_date, 'MONTH') > (SELECT rpt_dt FROM max_prt_dt);

;
/