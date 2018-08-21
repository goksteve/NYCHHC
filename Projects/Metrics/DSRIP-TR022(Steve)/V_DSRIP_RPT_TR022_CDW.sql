CREATE OR REPLACE VIEW v_dsrip_rpt_tr022_cdw AS
 SELECT
  dsrip_report,
  report_dt,
  network,
  facility_name,
  pat_lname,
  pat_fname,
  mrn,
  birthdate,
  age,
  apt_suite,
  street_address,
  city,
  state,
  country,
  mailing_code,
  home_phone,
  day_phone,
  pcp,
  visit_number,
  visit_type_id,
  visit_type,
  admission_dt,
  discharge_dt,
  medicaid_ind,
  payer_group,
  payer_name,
  plan_id,
  plan_name,
  diabetes_flag,
  diab_medication_flag,
  kidney_diag_num_flag,
  eye_exam_num_flag,
  eye_exam_latest_result_dt,
  eye_exam_result,
  nephropathy_num_flag,
  nephropathy_latest_result_dt,
  hba1c_num_flag,
  hba1c_latest_result,
  hba1c_latest_result_dt,
  ace_arb_ind,
  pcp_bh_flag,
  TRUNC(pcp_bh_service_dt) pcp_bh_service_dt
 FROM
  dsrip_tr022_diab_screen_cdw
 WHERE
  report_dt = (SELECT MAX(report_dt) FROM dsrip_tr022_diab_screen_cdw)
  AND (admission_dt <=
        CASE
         WHEN network = 'QHN' THEN DATE '2016-04-01'
         WHEN network = 'SBN' THEN DATE '2017-02-25'
         ELSE (SELECT MAX(report_dt) FROM dsrip_tr022_diab_screen_cdw) END )
 ORDER BY
  network, facility_name, pat_lname;