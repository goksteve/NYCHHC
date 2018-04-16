DROP VIEW V_TR017_DIAB_MON_CDW;

CREATE OR REPLACE FORCE VIEW v_tr017_diab_mon_cdw
(
 "Report Month",
 network,
 patient_id,
 facility_id,
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
 last_pcp_visit_date,
 visit_type,
 admission_date_time,
 discharge_date_time,
 last_bh_facility,
 last_bh_visit_date,
 last_bh_provider,
 medicaid_ind,
 payer_group,
 payer_id,
 payer_name,
 plan_name,
 icd_code,
 diagnose_desc,
 ldl_result_date,
 ldl_result_value,
 a1c_result_date,
 a1c_result_value,
 comb_ind,
 a1c_ind,
 ldl_ind
) AS
 SELECT
  p.report_month_dt AS "Report Month",
  p.network,
  p.patient_id,
  p.facility_id,
  p.facility_name,
  p.pat_lname,
  p.pat_fname,
  p.mrn,
  p.birthdate,
  p.age,
  pd.apt_suite,
  pd.street_address,
  pd.city,
  pd.state,
  pd.country,
  pd.mailing_code,
  pd.home_phone,
  pd.day_phone,
  p.pcp,
  last_pcp_visit_date,
  p.visit_type,
  p.admission_date_time,
  p.discharge_date_time,
  last_bh_facility,
  last_bh_visit_date,
  last_bh_provider,
  p.medicaid_ind,
  p.payer_group,
  p.payer_id,
  p.payer_name,
  p.plan_name,
  p.icd_code,
  p.diagnose_desc,
  c.ldl_result_date,
  c.ldl_result_value,
  h.a1c_result_date,
  h.a1c_result_value,
  p.comb_ind,
  p.a1c_ind,
  p.ldl_ind
 FROM
  (
   SELECT
    report_month_dt,
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
    last_pcp_visit_date,
    visit_type,
    admission_date_time,
    discharge_date_time,
    last_bh_facility,
    last_bh_visit_date,
    last_bh_provider,
    medicaid_ind,
    payer_group,
    payer_id,
    payer_name,
    plan_name,
    icd_code,
    diagnose_desc,
    comb_ind,
    a1c_ind,
    ldl_ind,
    ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY result_date DESC) cnt
   FROM
    dsrip_tr017_diab_mon_cdw
   WHERE
    report_month_dt = (SELECT MAX(report_month_dt) FROM dsrip_tr017_diab_mon_cdw)
  ) p
  LEFT JOIN
  (
   SELECT
    network,
    patient_id,
    result_date AS ldl_result_date,
    result_value AS ldl_result_value,
    test_type
   FROM
    dsrip_tr017_diab_mon_cdw
   WHERE
    test_type = 'LDL' AND report_month_dt = (SELECT MAX(report_month_dt) FROM dsrip_tr017_diab_mon_cdw)
  ) c
   ON p.network = c.network AND p.patient_id = c.patient_id
  LEFT JOIN
  (
   SELECT
    network,
    patient_id,
    result_date AS a1c_result_date,
    result_value AS a1c_result_value,
    test_type
   FROM
    dsrip_tr017_diab_mon_cdw
   WHERE
    test_type = 'A1C' AND report_month_dt = (SELECT MAX(report_month_dt) FROM dsrip_tr017_diab_mon_cdw)
  ) h
   ON p.network = h.network AND p.patient_id = h.patient_id
  LEFT JOIN dim_patients pd ON c.network = pd.network AND c.patient_id = pd.patient_id AND current_flag = 1
 WHERE
  p.cnt = 1;
