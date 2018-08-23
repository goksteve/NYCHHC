CREATE OR REPLACE FORCE VIEW v_dsrip_report_tr038
(
 network,
 name,
 medical_record_number,
 sex,
 birth_date,
 apt_suite,
 street_address,
 city,
 state,
 country,
 mailing_code,
 home_phone,
 cell_number,
 age_on_adm_dt,
 visit_id,
 visit_segment_number,
 patient_id,
 visit_type_id,
 visit_type,
 visit_status_id,
 visit_status,
 facility_id,
 facility_name,
 attending_provider,
 resident_provider,
 admission_date_time,
 discharge_date_time,
 discharge_type_id,
 financial_class_id,
 financial_class,
 payer_number,
 payer_id,
 plan_number,
 payer_name,
 is_medicaid_ptnt,
 prim_care_provider,
 coding_scheme,
 dgns_description,
 admin_medication,
 dschrg_medication,
 discharge_proc_desc,
 discharge_type,
 discharge_summary_date,
 numerator,
 age_group,
 rpt_strt_dt,
 rpt_end_dt,
 epic_flag,
 load_date
) AS
 SELECT
  "NETWORK",
  "NAME",
  "MEDICAL_RECORD_NUMBER",
  "SEX",
  "BIRTH_DATE",
  "APT_SUITE",
  "STREET_ADDRESS",
  "CITY",
  "STATE",
  "COUNTRY",
  "MAILING_CODE",
  "HOME_PHONE",
  "CELL_NUMBER",
  "AGE_ON_ADM_DT",
  "VISIT_ID",
  "VISIT_SEGMENT_NUMBER",
  "PATIENT_ID",
  "VISIT_TYPE_ID",
  "VISIT_TYPE",
  "VISIT_STATUS_ID",
  "VISIT_STATUS",
  "FACILITY_ID",
  "FACILITY_NAME",
  "ATTENDING_PROVIDER",
  "RESIDENT_PROVIDER",
  "ADMISSION_DATE_TIME",
  "DISCHARGE_DATE_TIME",
  "DISCHARGE_TYPE_ID",
  "FINANCIAL_CLASS_ID",
  "FINANCIAL_CLASS",
  "PAYER_NUMBER",
  "PAYER_ID",
  "PLAN_NUMBER",
  "PAYER_NAME",
  "IS_MEDICAID_PTNT",
  "PRIM_CARE_PROVIDER",
  "CODING_SCHEME",
  "DGNS_DESCRIPTION",
  "ADMIN_MEDICATION",
  "DSCHRG_MEDICATION",
  "DISCHARGE_PROC_DESC",
  "DISCHARGE_TYPE",
  "DISCHARGE_SUMMARY_DATE",
  "NUMERATOR",
  "AGE_GROUP",
  "RPT_STRT_DT",
  "RPT_END_DT",
  "EPIC_FLAG",
  "LOAD_DATE"
 FROM
  pt005.tr038_asthma_fnl;


GRANT SELECT ON V_DSRIP_REPORT_TR038 TO PUBLIC;
