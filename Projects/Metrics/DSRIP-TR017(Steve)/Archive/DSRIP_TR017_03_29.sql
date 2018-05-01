
ALTER SESSION ENABLE PARALLEL DDL;
ALTER SESSION ENABLE PARALLEL DML;
CREATE TABLE steve_del_gluc_ldl
NOLOGGING
COMPRESS BASIC
PARALLEL 32 AS
WITH report_dates AS
   (
     SELECT --+ materialize
     NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
     ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')),  -24)   start_dt,
     ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
     ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
    FROM
     DUAL
   ),
search_crit
AS(
    SELECT  network, criterion_id, VALUE AS elem_id,
    value_description AS elem_desc, include_exclude_ind
    FROM
     meta_conditions mc
    WHERE
     mc.criterion_id IN (4, 10) AND mc.condition_type_cd = 'EI'
  )
,
tmp_pcp_bh
AS(
    SELECT --+ PARALLEL (32)
      network,   visit_id,  patient_id,
      last_pcp_facility,  last_pcp_visit_dt,
      last_pcp_atn_provider_id,  last_pcp_atn_provider,
      last_bh_facility,  last_bh_visit_dt,
      last_bh_atn_provider_id,  last_bh_atn_provider
    FROM
    (
     SELECT --+ PARALLEL (32)
    *
    FROM
    (
      SELECT --+ PARALLEL (32)
        v.network, v.visit_id,
        v.patient_id, f.facility_name AS pcp_bh_facility,
        TRUNC(v.admission_dt) AS pcp_bh_visit_dt,
        p.provider_id AS attending_provider_id,
        NVL(p.provider_name, 'Uknown') AS attending_provider,
        d.service_type,
        ROW_NUMBER() OVER(PARTITION BY vs.visit_id, d.service_type ORDER BY v.admission_dt DESC) cnt
      FROM
        dim_hc_departments d
        JOIN fact_visit_segment_locations vs
        ON d.location_id = vs.location_id AND d.network = vs.network AND d.service_type IN ('PCP', 'BH')
        JOIN fact_visits v ON v.visit_id = vs.visit_id AND v.network = vs.network
        JOIN dim_hc_facilities f ON f.facility_key = d.facility_key
        LEFT JOIN dim_providers p ON p.provider_key = v.attending_provider_key
      WHERE
        v.admission_dt >= ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) AND v.admission_dt < TRUNC(SYSDATE, 'MONTH')
     )
    WHERE
    cnt = 1
    )
    PIVOT
    (MAX(pcp_bh_facility)   AS facility, MAX(pcp_bh_visit_dt)   AS visit_dt, MAX(attending_provider_id)
    AS atn_provider_id, MAX(attending_provider)   AS atn_provider   FOR service_type
    IN ('PCP' AS last_pcp, 'BH' AS last_bh))
  ),

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

