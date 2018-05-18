CREATE OR REPLACE VIEW v_dsrip_tr002_023_a1c_epic AS
 WITH max_prt_dt AS
       (SELECT
         MAX(NVL(report_month_dt, TO_DATE('01/01/2018', 'MM/DD.YYYY'))) rpt_dt
        FROM
         dsrip_tr002_023_a1c_epic)
 SELECT
  empi,
  facility_mrn,
  pat_last_name,
  pat_first_name,
  add_line_1,
  add_line_2,
  city,
  zip,
  state,
  pat_home_phone,
  pat_work_phone,
  birth_date,
  age_years,
  inspayor1,
  inspayor2,
  inspayor3,
  facility_latest,
  encounter_date_latest,
  result_time_latest,
  result_value_latest,
  DECODE(a1c_less_then_8, 'N', NULL, 1) AS a1c_less_then_8,
  DECODE(a1c_grt_eg_8, 'N', NULL, 1) AS a1c_grt_eg_8,
  DECODE(a1c_grt_eg_9, 'N', NULL, 1) AS a1c_grt_eg_9,
  DECODE(a1c_grt_eg_9_or_null, 'N', NULL, 1) AS a1c_grt_eg_9_or_null,
  pcp_general,
  TRUNC(etl_load_date) etl_load_date,
  epic_flag,
  source,
  TRUNC(etl_load_date, 'MONTH') AS report_month_dt
 FROM
  max_prt_dt a CROSS JOIN epic_clarity.dsrip_diabetes_hemo_labs
 WHERE
  TRUNC(etl_load_date, 'MONTH') > a.rpt_dt AND TRUNC(etl_load_date) > TRUNC(SYSDATE, 'MONTH');

COMMIT;