DROP VIEW v_dsrip_rpt_tr002_023_cdw;

CREATE OR REPLACE FORCE VIEW v_dsrip_rpt_tr002_023_cdw AS
 SELECT
 *
 FROM
 (WITH report_dates AS
        (SELECT --+ materialize
          NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
          ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')), -24)  start_dt,
          ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) last_12_mon,
          TRUNC(SYSDATE, 'MONTH') - 1 AS last_day_prev_month,
          ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS last_day_measur_year
         FROM
          DUAL),
       diab_diagnoses AS
        (SELECT --+ materialize
          d.network,
          d.patient_id,
          d.onset_date,
          d.diag_code icd_code,
          d.problem_comments,
          mc.include_exclude_ind,
          ROW_NUMBER() OVER(PARTITION BY patient_key, include_exclude_ind ORDER BY d.onset_date DESC) rnum
         FROM
          meta_conditions mc JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE
         WHERE
          mc.criterion_id = 1
          AND d.status_id IN (0,6,7, 8)),
       pat_list AS
        (SELECT
          p.*,
          rd.*,
          v.visit_id,
          v.facility_key,
          first_payer_key AS payer_key,
          final_visit_type_id AS visit_type_id,
          financial_class_id AS plan_id,
          v.admission_dt,
          v.discharge_dt,
          ROW_NUMBER() OVER(PARTITION BY p.network, p.patient_id ORDER BY v.admission_dt DESC) visit_rnum
         FROM
          report_dates rd
          CROSS JOIN fact_visits v
          JOIN (SELECT network, patient_id
                FROM   diab_diagnoses
                WHERE include_exclude_ind = 'I'
                MINUS
                SELECT   network, patient_id
                FROM   diab_diagnoses
                WHERE    include_exclude_ind = 'E') m
           ON m.patient_id = v.patient_id AND m.network = v.network
          JOIN dim_patients p
           ON p.patient_id = m.patient_id
              AND p.network = m.network
              AND p.current_flag = 1
     AND FLOOR((rd.last_day_measur_year - pp.birthdate) / 365) BETWEEN 18 AND 75
--              AND p.birthdate > ADD_MONTHS(rd.report_dt, -900)
--              AND p.birthdate <= ADD_MONTHS(rd.report_dt, -216)
         WHERE
              vv.admission_date_time BETWEEN rd.start_dt AND rd.report_dt
              AND vv.visit_status_id NOT IN (8,9,10,11) 
              AND vv.visit_type_id NOT IN (8,5,7,-1) 
)
   SELECT --+ parallel(32)
    pp.network,
    CASE WHEN r.a1c_final_calc_value < 8 THEN 1 ELSE NULL END AS a1c_less_8,
    CASE WHEN r.a1c_final_calc_value >= 8 THEN 1 ELSE NULL END AS a1c_more_8,
    CASE WHEN r.a1c_final_calc_value >= 9 THEN 1 ELSE NULL END AS a1c_more_9,
    CASE WHEN (NVL(r.a1c_final_calc_value, 9)) >= 9 THEN 1 ELSE NULL END AS a1c_more_9_null,
    pp.facility_key,
    f.facility_name,
    pp.patient_id,
    SUBSTR(pp.name, 1, INSTR(pp.name, ',', 1) - 1) AS pat_lname,
    SUBSTR(pp.name, INSTR(pp.name, ',') + 1) AS pat_fname,
    pp.medical_record_number AS mrn,
    pp.birthdate,
   ROUND((pp.admission_dt -pp.birthdate) / 365) AS age,
    pp.PCP_PROVIDER_NAME	AS pcp,
    pp.visit_id,
    pp.visit_type_id,
    tt.name AS visit_type,
    pp.admission_dt,
    pp.discharge_dt,
CASE UPPER(TRIM(dp.payer_group)) WHEN 'MEDICAID' THEN 'Y' ELSE NULL END AS medicaid_ind,
(CASE
WHEN UPPER(TRIM(dp.payer_group)) = 'MEDICAID' THEN
  'Medicaid'
WHEN UPPER(TRIM(dp.payer_group)) = 'MEDICARE' THEN
  'Medicare'
WHEN UPPER(TRIM(dp.payer_group)) = 'UNINSURED' THEN
  'Self pay'
WHEN NVL(TRIM(dp.payer_group), 'X') = 'X' THEN
  NULL
ELSE
  'Commercial'
END)
AS payer_group,
    pp.payer_key,
    dp.payer_name,
    pp.plan_id,
    fc.financial_class_name AS plan_name,
    dd.onset_date,
    dd.icd_code,
    dd.problem_comments,
    r.a1c_final_orig_value,
    r.a1c_final_calc_value,
    'DSRIP_TR002_023' AS dsrip_report,
    pp.report_dt,
    ROW_NUMBER() OVER(PARTITION BY pp.NETWORK, pp.patient_id ORDER BY pp.admission_dt DESC) result_rnum,
    TRUNC(SYSDATE) load_dt
   FROM   pat_list pp
    JOIN diab_diagnoses dd ON dd.patient_id = pp.patient_id AND dd.NETWORK = pp.NETWORK AND dd.rnum = 1
    JOIN dim_hc_facilities f ON f.facility_key = pp.facility_key
    LEFT JOIN fact_visit_metric_results r ON r.visit_id = pp.visit_id AND r.NETWORK = pp.NETWORK  AND r.a1c_final_orig_value IS NOT NULL
    LEFT JOIN dim_hc_facilities f ON f.facility_key = pp.facility_key
    LEFT JOIN ref_visit_types tt ON tt.visit_type_id = pp.visit_type_id
    LEFT JOIN dim_payers dp ON dp.payer_key = pp.payer_key
    LEFT JOIN ref_financial_class fc ON fc.NETWORK = pp.NETWORK AND fc.financial_class_id = pp.plan_id
   WHERE
    pp.visit_rnum = 1)
 WHERE
  result_rnum = 1
;