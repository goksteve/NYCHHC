CREATE OR REPLACE FORCE VIEW v_dsrip_rpt_tr002_023_cdw AS
 SELECT
  dsrip_report,
  report_dt,
  network,
  a1c_less_8,
  a1c_more_8,
  a1c_more_9,
  a1c_more_9_null,
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
  visit_type,
  admission_dt,
  discharge_dt,
  medicaid_ind,
  payer_group,
  payer_name,
  plan_name,
  onset_date,
  icd_code,
  problem_comments,
  a1c_final_orig_value
 FROM
  dsrip_tr002_023_a1c_cdw t
 WHERE
  TRUNC(t.report_dt) = (SELECT MAX(TRUNC(report_dt)) FROM dsrip_tr002_023_a1c_cdw)
 AND  admission_dt_key is not null;