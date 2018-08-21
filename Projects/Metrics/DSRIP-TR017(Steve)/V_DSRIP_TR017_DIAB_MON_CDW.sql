CREATE OR REPLACE VIEW V_DSRIP_TR017_DIAB_MON_CDW AS
WITH report_dates AS
      (SELECT --+ materialize
       -- ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)report_dt, 
       TRUNC(SYSDATE, 'MONTH') report_dt, 
        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24)     start_dt,
        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
        ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
       FROM
        DUAL),

 pat_diag AS
      (
        SELECT --+  materialize 
        d.network, d.patient_id, mc.criterion_id AS crit_id, mc.include_exclude_ind AS ind
       FROM   meta_conditions mc JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE
       WHERE  mc.criterion_id IN (1, 31)  AND d.status_id IN (0, 6, 7,   8)),


sel_pat_diag
as(
SELECT  --+ materialize 
 p.network,
 p.patient_id
 FROM
 (
  (
    (
      SELECT network,  patient_id FROM pat_diag
      WHERE   crit_id = 1 AND ind = 'I'
      UNION
      SELECT  d.network,patient_id as patient_id
      FROM fact_patient_prescriptions d
      JOIN ref_drug_descriptions rd
      ON TRIM(rd.drug_description) = TRIM(d.drug_description) AND rd.drug_type_id = 33
     )
       INTERSECT
        SELECT DISTINCT network, patient_id
        FROM pat_diag WHERE  crit_id = 31 AND ind = 'I'
   )
   MINUS
    SELECT DISTINCT network, patient_id
    FROM pat_diag  WHERE   ind = 'E'
 )p
 ) ,


tmp_pcp_bh
AS(
    SELECT --+ materialize 
    network, 
    patient_id,
    last_pcp_facility, 
    last_pcp_visit_dt,
    last_pcp_provider_id,  
    last_pcp_provider,
    last_bh_facility, 
    last_bh_visit_dt,
    last_bh_provider_id,
    last_bh_provider
    FROM
    (
     SELECT 
    *
    FROM
    (
      select 
      v.network, 
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
        JOIN fact_visit_segment_locations vs ON d.location_id = vs.location_id AND d.NETWORK = vs.NETWORK AND d.service_type IN ('PCP', 'BH')
        JOIN fact_visits v ON v.visit_id = vs.visit_id AND v.network = vs.network
        JOIN sel_pat_diag p ON  p.network =  v.network and p.patient_id  = v.patient_id
        JOIN dim_hc_facilities f ON f.facility_key = d.facility_key
        LEFT JOIN dim_providers P ON p.provider_key = v.attending_provider_key
      WHERE
        v.admission_dt >= start_dt AND v.admission_dt < report_dt
     )
    WHERE
    cnt = 1
    )
    PIVOT
    (MAX(pcp_bh_facility)   AS facility, MAX(pcp_bh_visit_dt)   AS visit_dt, MAX(attending_provider_id)   AS provider_id, MAX(attending_provider)   AS provider 
   FOR service_type
    IN ('PCP' AS last_pcp, 'BH' AS last_bh))
  ),
pat_visits
AS
(
select --+ materialize
v.network,
v.patient_id,
v.visit_id,
v.visit_number,
v.final_visit_type_id ,
v.admission_dt,
v.discharge_dt,
v.facility_key,
v.financial_class_id,
v.attending_provider_key,
v.first_payer_key ,
v.visit_status_id,
d.report_dt,   
d.report_year,
d.start_dt,
res_start_date,
row_number() over (partition by v.network, v.patient_id order by v.admission_dt DESC) v_cnt
FROM  report_dates d
CROSS JOIN fact_visits v
JOIN dim_patients pp on pp.network = v.network and pp.patient_id  = v.patient_id and current_flag = 1
JOIN sel_pat_diag p ON  p.network =  v.network and p.patient_id  = v.patient_id
 WHERE
 v.admission_dt >= start_dt AND v.admission_dt < report_dt
 AND FLOOR((d.report_year - pp.birthdate) / 365) BETWEEN 18 AND 64 
    AND v.visit_status_id NOT IN (8,9,10,11)
    AND v.final_visit_type_id NOT IN (8,5,7,-1)
),
Pat_visit_a1c_ldl AS
    ( 
    select  --+ materialize
    v.network,
    v.patient_id,
    v.visit_id,
    v.visit_number,
    v.final_visit_type_id ,
    v.admission_dt,
    v.discharge_dt,
    v.facility_key,
    v.financial_class_id,
    v.attending_provider_key,
    v.first_payer_key ,
    v.visit_status_id,
    b.a1c_final_result_dt,
    b.a1c_final_calc_value,
    c.ldl_final_result_dt,
    c.ldl_final_calc_value,
    v.report_dt,   
    v.report_year,
    v.start_dt,
    v.res_start_date
    FROM 
    pat_visits v
    LEFT JOIN  (
              SELECT --+ materialize
              r.network, r.patient_id, a1c_final_result_dt, a1c_final_calc_value,
              ROW_NUMBER() OVER (PARTITION BY r.network, r.patient_id ORDER BY r.admission_dt DESC) a1c_cnt
              FROM fact_visit_metric_results r
              WHERE ( r.network, r.patient_id) IN ( SELECT DISTINCT network, patient_id FROM pat_visits)
              AND  a1c_final_calc_value IS NOT NULL
              ) b on  b.network = v.network and b.patient_id  = v.patient_id and a1c_cnt = 1
    LEFT JOIN (
                SELECT --+ materialize
                r.network, r.patient_id, ldl_final_result_dt, ldl_final_calc_value,
                ROW_NUMBER() OVER (PARTITION BY r.network, r.patient_id ORDER BY r.admission_dt DESC) ldl_cnt
                FROM fact_visit_metric_results r
                WHERE ( r.network, r.patient_id) IN ( SELECT DISTINCT network, patient_id FROM pat_visits)
                AND  LDL_final_calc_value IS NOT NULL
              ) c on  c.network = v.network and c.patient_id  = v.patient_id and ldl_cnt = 1
    where  v_cnt = 1
    ),

 tmp_res
as
(
    SELECT --+ materialize
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
     lst.last_pcp_facility,
     lst.last_pcp_visit_dt,
     lst.last_pcp_provider_id,
     lst.last_pcp_provider,
     lst.last_bh_facility,
     lst.last_bh_visit_dt,
     lst.last_bh_provider_id,
     lst.last_bh_provider,
     v.a1c_final_result_dt a1c_result_dt,
     DECODE(v.a1c_final_calc_value, 0 , NULL ,v.a1c_final_calc_value) a1c_result ,
     CASE when NVL (v.a1c_final_calc_value,0 ) > 1 then  1 END a1c_ind,
     v.ldl_final_result_dt ldl_result_dt ,
     DECODE(v.ldl_final_calc_value, 0 , NULL ,v.ldl_final_calc_value) ldl_result,
     CASE when NVL (v.ldl_final_calc_value,0 ) > 1 then  1 END ldl_ind,
     v.report_dt,
     v.report_year,
     TRIM(SYSDATE) load_dt
     FROM  Pat_visit_a1c_ldl v
     LEFT JOIN tmp_pcp_bh lst ON  v.NETWORK = lst.NETWORK AND v.patient_id = lst.patient_id
     LEFT JOIN dim_hc_facilities f   ON f.facility_key = v.facility_key
     LEFT JOIN ref_financial_class fc   ON fc.NETWORK = v.NETWORK AND fc.financial_class_id = v.financial_class_id
     LEFT JOIN dim_providers P ON p.provider_key = v.attending_provider_key
  )
--************************************************************
SELECT /*+ Parallel (32) */
 res.network,
 TO_NUMBER(TO_CHAR(res.admission_dt, 'YYYYMMDD')) AS admission_dt_key,
 res.visit_facility_id AS facility_id,
 res.visit_facility_name AS facility_name,
 res.patient_id,
 SUBSTR(pp.name, 1, INSTR(pp.name, ',', 1) - 1) AS pat_lname,
 SUBSTR(pp.name, INSTR(pp.name, ',') + 1) AS pat_fname,
 NVL(psn.second_mrn, pp.medical_record_number) AS mrn,
 pp.birthdate,
 ROUND((res.admission_dt - pp.birthdate) / 365) AS age,
 pp.apt_suite,
 pp.street_address,
 pp.city,
 pp.state,
 pp.country,
 pp.mailing_code,
 pp.home_phone,
 pp.day_phone,
 pp.pcp_provider_name AS pcp,
 res.visit_id,
 res.visit_number,
 res.visit_type_id,
 vt.name AS visit_type,
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
-- res.test_type,
-- res.calc_result_value,
 res.last_pcp_facility,
 res.last_pcp_visit_dt,
 res.last_pcp_provider_id,
 res.last_pcp_provider,
 res.last_bh_facility,
 res.last_bh_visit_dt,
 res.last_bh_provider_id,
 res.last_bh_provider,
 CASE WHEN a1c_ind + ldl_ind  > 1 THEN 1 END AS comb_ind,
 CASE WHEN a1c_ind + ldl_ind  > 1 then NULL ELSE  a1c_ind END AS a1c_ind,
 CASE  WHEN a1c_ind + ldl_ind  > 1 then NULL ELSE  ldl_ind END AS ldl_ind,
 a1c_result_dt,
 a1c_result,
 ldl_result_dt,
 ldl_result,
'DSRIP_TR017_DIABETES_MONITORING' As DSRIP_REPORT,
 res.report_dt,
 res.load_dt
FROM tmp_res res
 JOIN dim_patients pp on pp.network = res.network and pp.patient_id  = res.patient_id and current_flag = 1
 AND FLOOR((res.report_year - pp.birthdate) / 365) BETWEEN 18 AND 64
LEFT JOIN dim_payers pm on pm.payer_key  = res.payer_key
LEFT JOIN ref_visit_types vt ON vt.visit_type_id  = res.visit_type_id
LEFT JOIN ref_patient_secondary_mrn psn  ON   psn.NETWORK = res.NETWORK AND psn.patient_id = res.patient_id AND psn.facility_id = res.visit_facility_id;
