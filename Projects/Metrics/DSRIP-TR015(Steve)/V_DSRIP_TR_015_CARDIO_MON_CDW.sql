--truncate table DSRIP_TR_015_CARDIO_MON_CDW;
--INSERT /*+ APPEND  PARALLEL(32) */
--     INTO
-- DSRIP_TR_015_CARDIO_MON_CDW
-- SELECT /* parallel(32) */
--  *
-- FROM
--  V_DSRIP_TR_015_CARDIO_MON_CDW



CREATE OR REPLACE VIEW V_DSRIP_TR_015_CARDIO_MON_CDW AS
WITH report_dates AS
      (SELECT --+ materialize
       -- ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)report_dt, 
       TRUNC(SYSDATE, 'MONTH') report_dt, 
        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24)     start_dt,
        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
        ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
       FROM
        DUAL),
 pat_diag
 AS
(
 SELECT --+  materialize
 d.network,  d.patient_id,
 diag_code, problem_comments, mc.criterion_id AS crit_id,
 mc.include_exclude_ind AS ind,
 row_number() over (partition by  d.network, d.patient_id, mc.criterion_id  order by  onset_date DESC) cnt
 FROM  meta_conditions mc JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE
WHERE  mc.criterion_id IN (30, 31) AND d.status_id IN (0,6,7,8)
),

Final_pat_diag
AS
(
  SELECT    pg.network, pg.patient_id ,
  LISTAGG(pg.diag_code, '-/- ') WITHIN GROUP (ORDER BY pg.diag_code) AS latest_diag_codes, 
  LISTAGG(pg.problem_comments, '-/- ')  WITHIN GROUP (ORDER BY pg.diag_code) AS latest_problem_comments 
  FROM
(
   SELECT * FROM
    (
      SELECT
      p.*,
      COUNT(DISTINCT crit_id) OVER (PARTITION BY network,patient_id) pat_rslt_cnt
      FROM pat_diag P WHERE cnt  = 1
    )
    where pat_rslt_cnt > 1
 ) pg
GROUP BY pg.network, pg.patient_id
),
visit_pat_diag
as(
SELECT --+ materialize
 v.network,
 f.facility_id,
 f.facility_name,
 v.patient_id,
 v.visit_id,
 v.visit_number,
 v.final_visit_type_id AS visit_type_id,
 v.admission_dt,
 v.discharge_dt,
 v.financial_class_id AS plan_id,
 fc.financial_class_name AS plan_name,
 v.first_payer_key AS payer_key,
 r.latest_diag_codes,
 latest_problem_comments,
 d.report_dt,
 d.report_year,
 row_number() over (partition by  v.network, v.patient_id   order by admission_dt DESC) pat_cnt
FROM
  report_dates d
CROSS JOIN 
     fact_visits v
     JOIN Final_pat_diag  R ON r.NETWORK = v.NETWORK AND r.patient_id =  v.patient_id
     LEFT JOIN dim_hc_facilities f   ON f.facility_key = v.facility_key
     LEFT JOIN ref_financial_class fc   ON fc.network = v.network AND fc.financial_class_id = v.financial_class_id
 WHERE admission_dt >= start_dt AND admission_dt < report_dt
),
res_ldl AS
(
    SELECT --+ materialize
   r.network,
   r.patient_id,
   ldl_final_result_dt AS ldl_test_dt,
   ldl_final_result_dt AS ldl_result_dt,
   ldl_final_orig_value AS orig_result_value,
   ldl_final_calc_value AS calc_result_value,
   ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY admission_dt DESC) cnt
FROM
 report_dates d CROSS JOIN fact_visit_metric_results r
WHERE
 admission_dt >= start_dt AND admission_dt < report_dt AND ldl_final_orig_value IS NOT NULL
    
),
res_ldl_diag
AS
(
SELECT --+ materialize
 p.network,
 to_number(to_char(p.admission_dt,'YYYYMMDD'))As admission_dt_key,
 p.facility_id,
 p.facility_name,
 p.patient_id,
 p.visit_id,
 p.visit_number,
 p.visit_type_id,
 p.admission_dt,
 p.discharge_dt,
 p.plan_id,
 p.plan_name,
 p. payer_key,
 p.latest_diag_codes,
 p.latest_problem_comments,
 l.ldl_test_dt,
 l.ldl_result_dt,
 l.orig_result_value,
 l.calc_result_value,
 p.report_dt,
 p.report_year
FROM
 visit_pat_diag p
left join res_ldl l ON l.network  = p.network and l.patient_id  = p.patient_id and l.cnt  = 1 
Where 
p.pat_cnt = 1
)

SELECT /*+ Parallel (32 */
 res.network,
 TO_NUMBER(TO_CHAR(res.admission_dt, 'YYYYMMDD')) AS admission_dt_key,
 res.facility_id,
 res.facility_name,
 res.patient_id,
 SUBSTR(pp.name, 1, INSTR(pp.name, ',', 1) - 1) AS pat_lname,
 SUBSTR(pp.name, INSTR(pp.name, ',') + 1) AS pat_fname,
 NVL(psn.secondary_number, pp.medical_record_number) AS mrn,
 pp.birthdate,
 ROUND((res.report_year - pp.birthdate) / 365) AS age,
 pp.apt_suite,
 pp.street_address,
 pp.city,
 pp.STATE,
 pp.country,
 pp.mailing_code,
 pp.home_phone,
 pp.day_phone,
 pp.pcp_provider_name AS pcp,
 res.visit_id,
 res.visit_number,
 res.visit_type_id,
 vt.NAME AS visit_type,
res.admission_dt,
res.discharge_dt,
CASE UPPER(TRIM(pm.payer_group)) WHEN 'MEDICAID' THEN 'Y' ELSE NULL END AS medicaid_ind,
           (CASE
               WHEN UPPER(TRIM(pm.payer_group)) = 'MEDICAID' THEN
                  'Medicaid'
               WHEN UPPER(TRIM(pm.payer_group)) = 'MEDICARE' THEN
                  'Medicare'
               WHEN UPPER(TRIM(pm.payer_group)) = 'UNINSURED' THEN
                  'Self pay'
               WHEN NVL(TRIM(pm.payer_group), 'X') = 'X' THEN
                  NULL
               ELSE
                  'Commercial'
            END)
              AS payer_group,
pm.payer_id,
pm.payer_name,
res.plan_id,
res.plan_name,
res.latest_diag_codes AS ICD_CODE,
res.latest_problem_comments,
res.ldl_test_dt,
res.ldl_result_dt,
res.orig_result_value,
res.calc_result_value,
'DSRIP_TR015_CARDIO_MONITORING' as dsrip_report,
res.report_dt,
 trim(sysdate) as load_dt
 FROM 
res_ldl_diag res
JOIN dim_patients pp on pp.network = res.network and pp.patient_id  = res.patient_id and current_flag = 1
 AND FLOOR((res.report_year - pp.birthdate) / 365) BETWEEN 18 AND 64
LEFT JOIN ref_visit_types vt ON vt.visit_type_id  = res.visit_type_id
LEFT JOIN dim_payers pm on pm.payer_key  = res.payer_key
LEFT JOIN patient_secondary_number psn
             ON     psn.network = res.network
                AND psn.patient_id = res.patient_id
                AND psn.secondary_nbr_type_id =
                       CASE
                          WHEN (res.network = 'GP1' AND res.facility_id = 1) THEN 13
                          WHEN (res.network = 'GP1' AND res.facility_id IN (2, 4)) THEN 11
                          WHEN (res.network = 'GP1' AND res.facility_id = 3) THEN 12
                          WHEN (res.network = 'CBN' AND res.facility_id = 4) THEN 12
                          WHEN (res.network = 'CBN' AND res.facility_id = 5) THEN 13
                          WHEN (res.network = 'NBN' AND res.facility_id = 2) THEN 9
                          WHEN (res.network = 'NBX' AND res.facility_id = 2) THEN 11
                          WHEN (res.network = 'QHN' AND res.facility_id = 2) THEN 11
                          WHEN (res.network = 'SBN' AND res.facility_id = 1) THEN 11
                          WHEN (res.network = 'SMN' AND res.facility_id = 2) THEN 11
                          WHEN (res.network = 'SMN' AND res.facility_id = 7) THEN 13
                          WHEN (res.network = 'SMN' AND res.facility_id = 8) THEN 14
                          WHEN (res.network = 'SMN' AND res.facility_id = 9) THEN 17
                       END