DROP VIEW V_TR002_TR023_HBA1C_CDW;

CREATE OR REPLACE FORCE VIEW V_TR002_TR023_HBA1C_CDW
(
 report_month,
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
 admission_date_time,
 discharge_date_time,
 medicaid_ind,
 payer_group,
 payer_name,
 financial_class,
 onset_date,
 icd_code,
 diagnose_desc,
 result_date,
 result_value
) AS
 SELECT /*+ PARALLEL (32) */
  t.report_month_dt AS report_month,
  t.network,
  t.a1c_less_8,
  t.a1c_more_8,
  t.a1c_more_9,
  t.a1c_more_9_null,
  t.facility_name,
  t.pat_lname,
  t.pat_fname,
  t.mrn,
  t.birthdate,
  t.age,
  pd.apt_suite,
  pd.street_address,
  pd.city,
  pd.state,
  pd.country,
  pd.mailing_code,
  pd.home_phone,
  pd.day_phone,
  pcp,
  visit_type,
  admission_date_time,
  discharge_date_time,
  medicaid_ind,
  payer_group,
  payer_name,
  plan_name AS financial_class,
  onset_date,
  icd_code,
  diagnose_desc,
  result_date,
  result_value
 FROM
  dsrip_tr002_023_hba1c_8_9_cdw t
  LEFT JOIN patient_dimension pd
   ON t.network = pd.network AND t.patient_id = pd.patient_id AND current_flag = 1
 WHERE
  TRUNC(t.report_month_dt) = (SELECT MAX(TRUNC(report_month_dt)) FROM dsrip_tr002_023_hba1c_8_9_cdw)
 ORDER BY
  network, facility_name, pat_lname;