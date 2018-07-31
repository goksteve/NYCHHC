CREATE OR REPLACE VIEW v_dsrip_rpt_tr015_cardio_cdw AS
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
  visit_type,
  admission_dt AS latest_admission_dt,
  discharge_dt,
  medicaid_ind,
  payer_group,
  payer_id,
  payer_name,
  plan_name,
  icd_code,
  problem_comments,
  ldl_test_dt,
  ldl_result_dt,
  --orig_result_value,
  calc_result_value
 FROM
  dsrip_tr015_cardio_mon_cdw
 WHERE
  report_dt = (SELECT MAX(report_dt) FROM dsrip_tr015_cardio_mon_cdw)