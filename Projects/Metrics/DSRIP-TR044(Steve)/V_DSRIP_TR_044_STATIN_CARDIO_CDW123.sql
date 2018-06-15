CREATE OR REPLACE VIEW V_DSRIP_TR_044_STATIN_CARDIO_CDW AS
--****************************************************************
--SG 2018-June 15 
--****************************************************************

--- *******DENOMINATOR --INCLUSIONS   MALES (21-75 YEARS OLD) AND FEMALES (40-75 YEARS OLD)  ************
--71	DIAGNOSES:ISCHEMIC VASCULAR DISEASE (IVD)	List The list of Ischemic Vascular (IVD) diagnoses
--70	DIAGNOSES:MYOCARDIAL INFARCTION (MI)	List The list of Myocardial Infarction (MI) diagnoses

-- *******DENOMINATOR  EXCLUSONS ************************************************************************
--73	DIAGNOSES:PREGNANCY ICD CODES	The List Pregnancy Icd Codes Diagnoses
--85	DIAGNOSES:MYALGIA, MYOSITIS, MYOPATHY
--84	DIAGNOSES:CIRRHOSIS ICD CODES
--83	DIAGNOSES:END-STAGE RENAL DISEASES
--86	MEDICATIONS:CLOMIFENE TREATS INFERTILITY IN WOMEN
--************* NUMERATOR	******************************************************************************
--72	MEDICATIONS:STATIN MEDICATIONS	The list of Statin Medications
--*******************************************************************************************************

WITH report_dates AS
    (SELECT --+ materialize
    -- ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)report_dt, 
    TRUNC(SYSDATE, 'MONTH') report_dt, 
    ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24)     start_dt,
    ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
    ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
    FROM
    DUAL
    ),
 pat_all_diag AS
    (
    SELECT --+  materialize 
    pd.network, pd.patient_id, pp.sex , FLOOR((d.report_year - pp.birthdate) / 365) AS age, pd.onset_date as dig_dt, 
    mc.criterion_id AS crit_id, mc.include_exclude_ind AS ind,
    row_number() over (partition by  pd.network, pd.patient_id, mc.criterion_id  order by  pd.onset_date DESC) cnt
    FROM
    report_dates d 
    CROSS JOIN  meta_conditions mc
    JOIN fact_patient_diagnoses pd ON pd.diag_code = mc.VALUE
    JOIN DIM_PATIENTS pp on pp.network  = pd.network and pp.patient_id  = pd.patient_id and current_flag  = 1
    WHERE  mc.criterion_id IN (70,71,73,85,84,83)  AND Pd.status_id IN (0, 6, 7,8)
    AND   FLOOR((d.report_year - pp.birthdate) / 365) BETWEEN  ( CASE WHEN LOWER(sex) = 'male' THEN  21 ELSE 40 END ) AND 75 
    ),

denom_pat
      AS
  (
    SELECT  --+ materialize 
    P.NETWORK,
    P.PATIENT_ID
    FROM
  (
      SELECT network, patient_id FROM pat_all_diag
      WHERE   crit_id in(70,71)  AND CNT  = 1
    MINUS
      (
        SELECT  network, patient_id
        FROM pat_all_diag  WHERE crit_id in(73,85,84,83) AND CNT  = 1
      UNION  
        SELECT DISTINCT d.network, d.patient_id
        FROM  report_dates d
        CROSS JOIN fact_patient_prescriptions d
        JOIN ref_drug_descriptions rd
        ON rd.drug_description = d.drug_description AND rd.drug_type_id = 86
        WHERE
        order_dt >= start_dt
      )
    ) p
  ),

select *  from fact_visits


numerator_pat
AS
(
 SELECT DISTINCT d.network, d.patient_id
        FROM  report_dates d
        CROSS JOIN fact_patient_prescriptions d
        JOIN ref_drug_descriptions rd
        ON rd.drug_description = d.drug_description AND rd.drug_type_id = 72
        WHERE
        order_dt >= start_dt
) --

select 
NETWORK,
 SUBSTR(pp.name, 1, INSTR(pp.name, ',', 1) - 1) AS pat_lname,
 SUBSTR(pp.name, INSTR(pp.name, ',') + 1) AS pat_fname,
  MRN                   
  BIRTHDATE             
  AGE                   
  APT_SUITE             
  STREET_ADDRESS        
  CITY                  
  STATE                 
  COUNTRY               
  MAILING_CODE          
  HOME_PHONE            
  DAY_PHONE             
Financial Class
Insurance/Payor 1
Insurance/Payor 2
Insurance/Payor 3

PCP name
PCP facility
Primary Care Clinic (see ""Primary Care_Cardiology Clinics"" tab)
Date of last PCP visit
Name of provider at last PCP visit
Last PCP visit facility
MI diagnosis name 
Date of most recent MI diagnosis
Ischemic Vascular Disease diagnosis name
Latest Ischemic Vascular Disease diagnosis date (July 2017 – June 2018)
Latest Ischemic Vascular Disease diagnosis date (July 2016 – June 2017)
Date of prescribed statin
Name of Prescribed Statin
Quantity
Number of Refills
Name of provider who prescribed the statin
Facility of provider who prescribed the statin
Date of last visit with non-PCP provider
Name of provider of last visit with a non PCP-Provider
Specialty/type of provider of last visit with a non-PCP provider
Date of Last Cardiology Appointment  (see ""Primary Care_Cardiology Clinics"" tab)
Name of Cardiology Provider
Numerator Flag (Y/N)
REPORT          VARCHAR2(255 BYTE)          NULL,
REPORT_DT             DATE                        NULL,
LOAD_DT               DATE                    DEFAULT SYSDATE                   NULL
























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
a1c_ldl AS
(
   SELECT  --+ materialize  
    network, visit_id,
   patient_id, admission_dt,
   test_type,
   calc_result_value, report_dt, report_year,
   ROW_NUMBER() OVER(PARTITION BY network, patient_id, test_type ORDER BY admission_dt DESC) cnt
  FROM
   (
     SELECT network, visit_id,
     patient_id, admission_dt, test_type,
     calc_result_value,
     report_dt, report_year
    FROM
     (
      SELECT --+ materialize
       r.network, visit_id,   r.patient_id,
       admission_dt,
       a1c_final_calc_value,
       ldl_final_calc_value,d.report_dt,   d.report_year
      FROM
      report_dates d
      CROSS JOIN fact_visit_metric_results r
      JOIN sel_pat_diag p ON  p.network =  r.network and p.patient_id  = r.patient_id
      WHERE
        admission_dt >= start_dt AND admission_dt < report_dt
    AND( ldl_final_calc_value IS NOT NULL OR a1c_final_calc_value IS NOT NULL)
     )
   UNPIVOT
    (calc_result_value
    FOR test_type
    IN (a1c_final_calc_value AS 'A1C',
       ldl_final_calc_value AS  'LDL')
   )
)
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
     NVL(r.test_type,'NONE') as test_type,
     r.calc_result_value,
     v.report_dt,
     v.report_year,
     TRIM(SYSDATE) load_dt,
     COUNT(DISTINCT NVL(r.test_type,'NONE')) OVER (PARTITION BY r.NETWORK , r.patient_id) pat_rslt_cnt
     FROM  pat_visits v
     LEFT JOIN (SELECT * FROM  a1c_ldl  where cnt  = 1)r    ON r.NETWORK = v.NETWORK AND r.patient_id = v.patient_id
     LEFT JOIN tmp_pcp_bh lst ON  v.NETWORK = lst.NETWORK AND v.patient_id = lst.patient_id
     LEFT JOIN dim_hc_facilities f   ON f.facility_key = v.facility_key
     LEFT JOIN ref_financial_class fc   ON fc.NETWORK = v.NETWORK AND fc.financial_class_id = v.financial_class_id
     LEFT JOIN dim_providers P ON p.provider_key = v.attending_provider_key
    WHERE v_cnt = 1
   
  )
    

--************************************************************

SELECT /*+ Parallel (32) */
 res.network,
 TO_NUMBER(TO_CHAR(res.admission_dt, 'YYYYMMDD')) AS admission_dt_key,
 CASE WHEN res.pat_rslt_cnt > 1 THEN 1 END AS comb_ind,
 CASE WHEN pat_rslt_cnt < 2 AND test_type = 'A1C'   THEN 1 END AS a1c_ind,
 CASE WHEN pat_rslt_cnt < 2 AND test_type = 'LDL'   THEN 1 END AS ldl_ind,
 --CASE WHEN pat_rslt_cnt < 2 AND test_type = 'NONE'   THEN 1 END AS NO_ind,
 res.visit_facility_id AS facility_id,
 res.visit_facility_name AS facility_name,
 res.patient_id,
 SUBSTR(pp.name, 1, INSTR(pp.name, ',', 1) - 1) AS pat_lname,
 SUBSTR(pp.name, INSTR(pp.name, ',') + 1) AS pat_fname,
 NVL(psn.secondary_number, pp.medical_record_number) AS mrn,
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
 res.test_type,
 res.calc_result_value,
 res.last_pcp_facility,
 res.last_pcp_visit_dt,
 res.last_pcp_provider_id,
 res.last_pcp_provider,
 res.last_bh_facility,
 res.last_bh_visit_dt,
 res.last_bh_provider_id,
 res.last_bh_provider,
 'DSRIP_TR017_DIABETES_MONITORING' As DSRIP_REPORT,
 res.report_dt,
 res.load_dt
FROM tmp_res res
 JOIN dim_patients pp on pp.network = res.network and pp.patient_id  = res.patient_id and current_flag = 1
 AND FLOOR((res.report_year - pp.birthdate) / 365) BETWEEN 18 AND 64
LEFT JOIN dim_payers pm on pm.payer_key  = res.payer_key
LEFT JOIN ref_visit_types vt ON vt.visit_type_id  = res.visit_type_id
LEFT JOIN patient_secondary_number psn
             ON     psn.network = res.network
                AND psn.patient_id = res.patient_id
                AND psn.secondary_nbr_type_id =
                       CASE
                          WHEN (res.network = 'GP1' AND res.visit_facility_id = 1) THEN 13
                          WHEN (res.network = 'GP1' AND res.visit_facility_id IN (2, 4)) THEN 11
                          WHEN (res.network = 'GP1' AND res.visit_facility_id = 3) THEN 12
                          WHEN (res.network = 'CBN' AND res.visit_facility_id = 4) THEN 12
                          WHEN (res.network = 'CBN' AND res.visit_facility_id = 5) THEN 13
                          WHEN (res.network = 'NBN' AND res.visit_facility_id = 2) THEN 9
                          WHEN (res.network = 'NBX' AND res.visit_facility_id = 2) THEN 11
                          WHEN (res.network = 'QHN' AND res.visit_facility_id = 2) THEN 11
                          WHEN (res.network = 'SBN' AND res.visit_facility_id = 1) THEN 11
                          WHEN (res.network = 'SMN' AND res.visit_facility_id = 2) THEN 11
                          WHEN (res.network = 'SMN' AND res.visit_facility_id = 7) THEN 13
                          WHEN (res.network = 'SMN' AND res.visit_facility_id = 8) THEN 14
                          WHEN (res.network = 'SMN' AND res.visit_facility_id = 9) THEN 17
                       END