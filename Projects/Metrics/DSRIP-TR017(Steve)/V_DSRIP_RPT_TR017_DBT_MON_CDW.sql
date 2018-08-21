CREATE OR REPLACE VIEW v_dsrip_rpt_tr017_dbt_mon_cdw AS
 SELECT
  dsrip_report,
  report_dt,
  network,
  patient_id,
  facility_id,
  facility_name,
  pat_lname,
  pat_fname,
  mrn,
  birthdate,
  age,
  pcp,
  last_pcp_visit_dt,
  visit_type,
  admission_dt AS latest_admission_dt,
  discharge_dt,
  last_bh_facility,
  last_bh_visit_dt,
  last_bh_provider,
  medicaid_ind,
  payer_group,
  payer_id,
  payer_name,
  plan_name,
  comb_ind,
  a1c_ind,
  ldl_ind,
  a1c_result_dt,
  a1c_result,
  ldl_result_dt,
  ldl_result
 FROM
  dsrip_tr017_diab_mon_cdw
 WHERE
  report_dt = (SELECT MAX(report_dt) FROM dsrip_tr017_diab_mon_cdw)