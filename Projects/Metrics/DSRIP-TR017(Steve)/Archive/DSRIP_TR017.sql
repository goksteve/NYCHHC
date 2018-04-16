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
Select  network,criterion_id, value as elem_id , value_description as elem_desc ,  include_exclude_ind
 from  meta_conditions mc   WHERE     mc.criterion_id IN (4, 10)
                                         AND mc.condition_type_cd = 'EI'  )
,
res_gluc_tmp AS
(
 SELECT --+ materialize  PARALLEL (32)
 v.network,
 f.facility_id,
 f.facility_name,
 v.patient_id,
 v.visit_id,
 v.final_visit_type_id AS visit_type_id,
 v.admission_dt,
 v.discharge_dt,
 v.financial_class_id AS plan_id,
 fc.financial_class_name AS plan_name,
 v.first_payer_key AS payer_key,
 r.event_id,
 mc.criterion_id,
DECODE(mc.criterion_id, 4, 'A1C', 'LDL') test_type,
r.result.dt,
rr. lem_id AS test_id,
mc.elem_desc AS test_desc,
CASE
WHEN mc.criterion_id = 10 THEN
     CASE WHEN LOWER(rr.VALUE) NOT LIKE '%not done%' AND LOWER(rr.VALUE) NOT LIKE '%unable%' THEN rr.VALUE END
WHEN     SUBSTR(rr.VALUE, 1, 1) <> '0'
       AND REGEXP_COUNT(rr.VALUE, '\.', 1) <= 1
      AND LENGTH(rr.VALUE) <= 38
     AND REGEXP_REPLACE(REGEXP_REPLACE(rr.VALUE, '[^[:digit:].]'), '\.$') <= 50 THEN
     REGEXP_REPLACE(REGEXP_REPLACE(rr.VALUE, '[^[:digit:].]'), '\.$')
END
AS result_value,
d.report_dt,
 d.report_year
FROM
 report_dates d

     CROSS JOIN fact_visits v
     JOIN fact_results r ON r.network = v.network AND r.visit_id = v.visit_id  AND r.VALUE IS NOT NULL
     JOIN  search_crit mc ON mc.elem_id  = r.data_element_id AND mc.NETWORK = r.NETWORK
     LEFT JOIN dim_hc_facilities f  ON f.facility_key = v.facility_key
     LEFT JOIN ref_financial_class fc ON fc.network = v.network  AND fc.financial_class_id = v.financial_class_id
  WHERE 
    v.admission_dt BETWEEN d.start_dt AND d.report_dt
    AND r.glucose_final_orig_value is not null 
    AND v.visit_status_id NOT IN (8,9,10,11)       --REMOVE ( cancelled,closed cancelled,no show,closed no show)
    AND v.final_visit_type_id NOT IN (8,5,7,-1) -- REMOVE(LIFECARE,REFFERAL,HISTORICAL,UNKNOWN)
),
res_ldl_tmp As
(
  SELECT /*+  materialize PARALLEL (32) */
    v.network,f.facility_id,f.facility_name,
    v.patient_id,v.visit_id,v.final_visit_type_id AS visit_type_id,
    v.admission_dt,v.discharge_dt,v.financial_class_id AS plan_id,
    fc.financial_class_name AS plan_name,v.first_payer_key as payer_key,'LDL' As test_type,
    r.ldl_final_orig_value AS final_orig_value,r.ldl_final_calc_value AS final_calc_value,
    ROW_NUMBER() OVER(PARTITION BY v.network, v.patient_id ORDER BY v.admission_dt DESC) res_count,
    d.report_dt,d.report_year
  FROM
    report_dates d
    CROSS JOIN fact_visits v
    JOIN fact_visit_metric_results r
    ON r.network = v.network AND r.visit_id = v.visit_id 
    LEFT JOIN dim_hc_facilities f  ON f.facility_key = v.facility_key
    LEFT JOIN ref_financial_class fc ON fc.network = v.network  AND fc.financial_class_id = v.financial_class_id
  WHERE 
    v.admission_dt BETWEEN d.start_dt AND d.report_dt
    AND  r.ldl_final_orig_value is not null 
    AND v.visit_status_id NOT IN (8,9,10,11) --REMOVE ( cancelled,closed cancelled,no show,closed no show)
    AND v.final_visit_type_id NOT IN (8,5,7,-1) -- REMOVE(LIFECARE,REFFERAL,HISTORICAL,UNKNOWN)
),

res_gluc_ldl
AS
(
 SELECT /*+  materialize PARALLEL (32) */
    network, facility_id, facility_name,
    patient_id, visit_id, visit_type_id,
    admission_dt, discharge_dt, plan_id,
    plan_name,payer_key, test_type, final_orig_value,
    final_calc_value, report_dt, report_year
 FROM
  res_gluc_tmp
 WHERE
  res_count = 1
UNION
SELECT
 network, facility_id, facility_name,
 patient_id, visit_id, visit_type_id,
 admission_dt, discharge_dt, plan_id,
 plan_name,payer_key, test_type, final_orig_value,
 final_calc_value, report_dt, report_year
FROM
 res_ldl_tmp
WHERE
 res_count = 1
)
SELECT --/*+ parallel(32)  */

 network,
 facility_id,
 facility_name,
 patient_id,
 visit_id,
 visit_type_id,
 admission_dt,
 discharge_dt,
 plan_id ,plan_name, payer_key,
 report_dt,
 report_year,
 gluc_final_orig_value,
 gluc_final_calc_value,
 ldl_final_orig_value,
 ldl_final_calc_value

FROM
 res_gluc_ldl
 PIVOT
  (MAX(final_orig_value) AS final_orig_value, MAX(final_calc_value) AS final_calc_value
  FOR test_type
 IN ('GLUC' as gluc, 'LDL' as ldl));


/*
SELECT
 v.network,
 v.visit_id,
 v.patient_id
 last_department_key,
 department_key,
 location_id,
 v.facility_key,
 specialty_code,
 specialty,
 service,
 service_type
FROM
 dim_hc_departments dep
join fact_visits v on v.last_department_key =  dep.department_key
where service_type  IN ('PCP', 'BH');*/


--r.ldl_final_orig_value  as final_orig_value ,
--r.ldl_final_calc_value  as Final_calc_value ,
--   CASE WHEN glucose_final_orig_value IS NOT NULL AND ldl_final_orig_value IS NOT NULL THEN 1 END AS comb_ind,
--   CASE WHEN glucose_final_orig_value IS NOT NULL AND ldl_final_orig_value IS NULL THEN 1 END AS gluc_ind,
--   CASE WHEN glucose_final_orig_value IS NULL AND ldl_final_orig_value IS not  NULL THEN 1 END AS ldl_ind,


--   CASE WHEN glucose_final_orig_value IS NOT NULL AND ldl_final_orig_value IS NOT NULL THEN 1 END AS comb_ind,
--   CASE WHEN glucose_final_orig_value IS NOT NULL AND ldl_final_orig_value IS NULL THEN 1 END AS gluc_ind,
--   CASE WHEN glucose_final_orig_value IS NULL AND ldl_final_orig_value IS not  NULL THEN 1 END AS ldl_ind,