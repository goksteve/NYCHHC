alter session enable parallel DDl;
create table steve_del_ldl_A1c
nologging
compress basic
parallel 32
AS



WITH report_dates AS
      (SELECT --+ materialize
        NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
        ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')), -24)
         start_dt,
        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
        ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
       FROM
        DUAL),
a1c_ldl AS
(
  SELECT --+ materialize
    network,
    visit_id,
    'A1C' AS test_type,
    a1c_final_orig_value AS orig_value,
    a1c_final_calc_value AS calc_value,
    d.report_dt,
    d.report_year
    FROM
    report_dates d CROSS JOIN fact_visit_metric_results
  WHERE
    a1c_final_orig_value IS NOT NULL 
    AND admission_dt BETWEEN d.start_dt AND d.report_dt
  UNION ALL
  SELECT
    network,
    visit_id,
    'LDL' AS test_type,
    ldl_final_orig_value AS orig_value,
    ldl_final_calc_value AS calc_value,
    d.report_dt,
    d.report_year
    FROM
    report_dates d CROSS JOIN fact_visit_metric_results
  WHERE
   ldl_final_orig_value IS NOT NULL
   AND admission_dt BETWEEN d.start_dt AND d.report_dt
),
  res_gluc_ldl
  AS
(
    SELECT /*+ parallel (32) */
     v.network,
     f.facility_id AS visit_facility_id,
     f.facility_name AS visit_facility_name,
     v.patient_id,
     v.visit_id,
     v.final_visit_type_id AS visit_type_id,
     v.admission_dt,
     v.discharge_dt,
     v.financial_class_id AS plan_id,
     fc.financial_class_name AS plan_name,
     v.first_payer_key AS payer_key,
     r.test_type,
     r.orig_value,
     r.calc_value,
     r.report_dt,
     r.report_year
    FROM  fact_visits v
     JOIN a1c_ldl r   ON r.network = v.network AND r.visit_id = v.visit_id
     LEFT JOIN dim_hc_facilities f   ON f.facility_key = v.facility_key
     LEFT JOIN ref_financial_class fc   ON fc.network = v.network AND fc.financial_class_id = v.financial_class_id
    WHERE
       v.visit_status_id NOT IN (8,9,10,11)
     AND v.final_visit_type_id NOT IN (8,5,7,-1)
  )
select /*+ parallel(32) */ * from res_gluc_ldl