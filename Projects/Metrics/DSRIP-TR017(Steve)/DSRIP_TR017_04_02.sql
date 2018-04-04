drop table steve_del_ldl_A1c;

alter session enable parallel DDL;
create table steve_del_ldl_A1c
nologging
compress basic
parallel 32
AS

WITH report_dates AS
      (SELECT --+ materialize
        TRUNC(SYSDATE, 'MONTH') report_dt, 
        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24)     start_dt,
        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
        ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
       FROM
        DUAL),
tmp_pcp_bh
AS(
     SELECT --+ PARALLEL (32)
    network, 
    visit_id, 
    patient_id,
    last_pcp_facility, 
    last_pcp_visit_dt,
    last_pcp_atn_provider_id,  
    last_pcp_atn_provider,
    last_bh_facility, 
    last_bh_visit_dt,
    last_bh_atn_provider_id,
    last_bh_atn_provider
    FROM
    (
     SELECT --+ PARALLEL (32)
    *
    FROM
    (
      select --+ PARALLEL (32)
v.network, 
v.visit_id, 
v.patient_id, 
f.facility_name as pcp_bh_facility,
trunc(v.admission_dt) as pcp_bh_visit_dt,
p.provider_id as attending_provider_id,
nvl(p.provider_name, 'Uknown') as attending_provider,
d.service_type,
ROW_NUMBER() OVER(PARTITION BY  v.network, v.patient_id, d.service_type ORDER BY v.admission_dt DESC) cnt
      FROM
         report_dates d
       CROSS JOIN  dim_hc_departments d
        JOIN fact_visit_segment_locations vs
        ON d.location_id = vs.location_id AND d.NETWORK = vs.NETWORK AND d.service_type IN ('PCP', 'BH')
        JOIN fact_visits v ON v.visit_id = vs.visit_id AND v.NETWORK = vs.NETWORK
        JOIN dim_hc_facilities f ON f.facility_key = d.facility_key
        LEFT JOIN dim_providers P ON p.provider_key = v.attending_provider_key
      WHERE
        v.admission_dt >= start_dt AND v.admission_dt < report_dt
     )
    WHERE
    cnt = 1
    )
    PIVOT
    (MAX(pcp_bh_facility)   AS facility, MAX(pcp_bh_visit_dt)   AS visit_dt, MAX(attending_provider_id)   AS atn_provider_id, MAX(attending_provider)   AS atn_provider 
   FOR service_type
    IN ('PCP' AS last_pcp, 'BH' AS last_bh))
  ),

a1c_ldl AS
(
SELECT  network, visit_id,
 patient_id, admission_dt,
 test_type, orig_result_value,
 calc_result_value, report_dt, report_year,
 ROW_NUMBER() OVER(PARTITION BY network, patient_id, test_type ORDER BY admission_dt DESC) cnt
FROM
 (
 SELECT network, visit_id,
 patient_id, admission_dt, test_type,
 orig_result_value, calc_result_value,
 report_dt, report_year
FROM
 (
  SELECT --+ materialize
   network, visit_id,   patient_id,
   admission_dt,a1c_final_orig_value,
   a1c_final_calc_value,ldl_final_orig_value,
   ldl_final_calc_value,d.report_dt,   d.report_year
  FROM
report_dates d
  CROSS JOIN fact_visit_metric_results
  WHERE
    admission_dt >= start_dt AND admission_dt < report_dt
AND( ldl_final_orig_value IS NOT NULL OR a1c_final_orig_value IS NOT NULL)
 )
 UNPIVOT
  ((orig_result_value, calc_result_value)
  FOR test_type
  IN ((a1c_final_orig_value, a1c_final_calc_value) AS 'A1C',
     (ldl_final_orig_value, ldl_final_calc_value) AS  'LDL'))
)
),

  res_gluc_ldl
as
(
  

     SELECT /*+ parallel (32) */
     v.network,
     f.facility_id AS visit_facility_id,
     f.facility_name AS visit_facility_name,
     v.patient_id,
     v.visit_id,
     v.visit_number,
     v.final_visit_type_id AS visit_type_id,
     v.admission_dt,
     v.discharge_dt,
     v.financial_class_id AS plan_id,
     fc.financial_class_name AS plan_name,
     v.first_payer_key AS payer_key,
     r.test_type,
     r.orig_result_value,
     r.calc_result_value,
     r.report_dt,
     r.report_year
     FROM  fact_visits v
     JOIN a1c_ldl r   ON r.network = v.network AND r.visit_id = v.visit_id
     LEFT JOIN dim_hc_facilities f   ON f.facility_key = v.facility_key
     LEFT JOIN ref_financial_class fc   ON fc.network = v.network AND fc.financial_class_id = v.financial_class_id
     LEFT JOIN dim_providers p ON p.provider_key = v.attending_provider_key
    WHERE
       v.visit_status_id NOT IN (8,9,10,11)
     AND v.final_visit_type_id NOT IN (8,5,7,-1)
    and r.cnt  = 1
  )

SELECT /*+ parallel(32) */
 network,
 visit_facility_id,
 visit_facility_name,
 patient_id,
 visit_id,
 visit_number,
 visit_type_id,
 admission_dt,
 discharge_dt,
 plan_id,
 plan_name,
 payer_key,
 last_pcp_facility,
 last_pcp_visit_dt,
 last_pcp_atn_provider_id,
 last_pcp_atn_provider,
 last_bh_facility,
 last_bh_visit_dt,
 last_bh_atn_provider_id,
 last_bh_atn_provider,
 report_dt,
 report_year,
 load_dt,
 a1c_final_orig_value,
 a1c_final_calc_value,
 ldl_final_orig_value,
 ldl_final_calc_value
FROM
(
  SELECT /*+ parallel(32) */
 res.network,
 res.visit_facility_id,
 res.visit_facility_name,
 res.patient_id,
 res.visit_id,
 res.visit_number,
 res.visit_type_id,
 res.admission_dt,
 res.discharge_dt,
 res.plan_id,
 res.plan_name,
 res.payer_key,
 lst.last_pcp_facility,
 lst.last_pcp_visit_dt,
 lst.last_pcp_atn_provider_id,
 lst.last_pcp_atn_provider,
 lst.last_bh_facility,
 lst.last_bh_visit_dt,
 lst.last_bh_atn_provider_id,
 lst.last_bh_atn_provider,
 res.test_type,
 res.orig_result_value,
 res.calc_result_value,
 res.report_dt,
 res.report_year,
 TRIM(SYSDATE) load_dt
FROM
 res_gluc_ldl res
    LEFT JOIN tmp_pcp_bh lst ON  res.network = last.network AND res.patient_id = lst.patient_id and  lst.visit_id  = res.visit_id
)
 PIVOT
(MAX(orig_result_value) AS final_orig_value, MAX(calc_result_value) AS final_calc_value
   FOR test_type
   IN ('A1C' AS a1c, 'LDL' AS ldl))