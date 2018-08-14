CREATE OR REPLACE VIEW v_dsrip_rpt_tr024_025_cdw AS
 SELECT
  dsrip_report,
  network,
  visit_number,
  visit_facility_name,
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
  initial_visit_type,
  visit_type,
  admission_dt,
  discharge_dt,
  service,
  medicaid_ind,
  payer_group,
  payer_id,
  payer_name,
  pcp,
  plan_name,
  pcp_bh_flag,
  pcp_bh_service_dt,
  icd_code,
  problem_comments,
  TRUNC(diagnosis_dt) diagnosis_dt,
  DECODE(prescriber_id, 999999999, NULL) AS prescriber_id,
  prescriber_name,
  prescriber_dept,
  drug_name,
  drug_description,
  dosage,
  frequency,
  daily_pills_cnt,
  rx_quantity,
  tr_024_num_flag,
  tr_025_num_flag,
  order_dt,
  next_order_dt,
  second_next_order_dt,
  third_next_order_dt,
  fourth_next_order_dt,
  fifth_next_order_dt,
  six_next_order_dt,
  seven_next_order_dt report_dt
 FROM
  dsrip_tr024_025_cdw
 WHERE
  report_dt = (SELECT MAX(report_dt) FROM dsrip_tr022_diab_screen_cdw)
  AND (admission_dt <=
        CASE
         WHEN network = 'QHN' THEN DATE '2016-04-01'
         WHEN network = 'SBN' THEN DATE '2017-02-25'
         ELSE (SELECT MAX(report_dt) FROM dsrip_tr022_diab_screen_cdw)
        END)
 ORDER BY
  network, visit_facility_name, pat_lname;