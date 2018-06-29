CREATE OR REPLACE VIEW V_DSRIP_TR_022_DIAB_SCREEN_CDW AS
--*CREATED 05-May-2018
--************************
--DIAGNOSES:DIAB MONITORING	                                 1	List of Diabetes diagnosis (monitoring)
--DIAGNOSES:NEPHROPATHY TREATMENT	                          63	List of Nephropathy Treatment diagnoses
--DIAGNOSES:KIDNEY DISEASES	                                65	List of Kidney Diseases, End-Stage Renal Diseases and Kidney Transplant diagnoses
--****************
--RESULTS:DIABETES A1C	  ( from table metric_results)                                   4	List of Procedures, Elements  for A1C tests
--RESULTS:NEPHROPATHY SCREEN_MONITOR	( from table metric_results)                      66	List of Nephropathy Screen Monitor tests
-- results eye exam     ( from table metric_results)                                    68 list if EYE exam elements
--********************
--MEDICATIONS:DIABETES             	                        33	List of Medications for treating Diabetes
--MEDICATIONS:ACE INHIBITOR/ARB CONTROL BLOOD PRESSURE	    64	List of Ace Inhibitor/Arb Medications to Control Blood Pressure

WITH
 report_dates AS
(
  SELECT --+ materialize
  -- ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)report_dt, 
  TRUNC(SYSDATE, 'MONTH') report_dt, 
  ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24)     start_dt,
  ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) rslt_start_date,
  ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
  FROM
  DUAL
),
pat_diag
AS
(
  select --+  materialize
  d.network, d.patient_id,  criterion_id as crit_id,   include_exclude_ind as ind 
  FROM    meta_conditions mc JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE
  WHERE
  mc.criterion_id IN (1,63,65)AND d.status_id IN (0, 6, 7,8)
 ) ,

denom_pat
AS
(
   SELECT /*+ parallel (32) */
   pp.network,
   v.visit_id,
   v.visit_number,
   v.facility_key,
   f.facility_id AS visit_facility_id,
   f.facility_name AS visit_facility_name,
   pp.patient_id ,
   SUBSTR(pp.name, 1, INSTR(pp.name, ',', 1) - 1) AS pat_lname,
   SUBSTR(pp.name, INSTR(pp.name, ',') + 1) AS pat_fname,
    NVL(psn.secondary_number, pp.medical_record_number) AS mrn,
   pp.apt_suite,
   pp.street_address,
   pp.city,
   pp.state,
   pp.country,
   pp.mailing_code,
   pp.home_phone,
   pp.day_phone,
   pp.birthdate,
  -- age
   v.initial_visit_type_id,
   v.final_visit_type_id AS visit_type_id,
   vt.name AS visit_type,
   v.admission_dt,
   v.discharge_dt,
  prov.PHYSICIAN_SERVICE_NAME_1 as Service,
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
    pp.pcp_provider_name AS pcp,
    v.financial_class_id AS plan_id,
    fc.financial_class_name AS plan_name,
    dt.report_dt,
    dt.start_dt,
    dt.rslt_start_date,
    row_number() over ( partition by  pp.network, pp.patient_id  order by  v.admission_dt desc) cnt
   FROM
   report_dates dt CROSS JOIN
  (
   (
      SELECT network, patient_id FROM pat_diag
      WHERE   crit_id = 1 AND ind = 'I'
    UNION
      SELECT  d.network, d.patient_id
      FROM report_dates dt 
      CROSS JOIN  fact_patient_prescriptions d
      JOIN ref_drug_descriptions rd
      ON TRIM(rd.drug_description) = TRIM(d.drug_description) AND rd.drug_type_id = 33
      WHERE order_dt >= dt.start_dt  AND order_dt < dt.report_dt
   )
    MINUS
        SELECT DISTINCT network, patient_id
      FROM pat_diag  WHERE  crit_id = 1 AND  ind = 'E'
   ) res
  JOIN dim_patients pp on pp.network = res.network and pp.patient_id  = res.patient_id and pp.current_flag = 1 AND FLOOR((dt.report_year - pp.birthdate) / 365) BETWEEN 18 AND 75
  JOIN fact_visits v on v.network = pp.network and v.patient_id  = pp.patient_id AND( v.admission_dt >=  dt.start_dt and  v.admission_dt <  dt.report_dt)
  LEFT JOIN dim_hc_facilities f   ON f.facility_key = v.facility_key
  LEFT JOIN dim_providers prov ON prov.provider_key = NVL(attending_provider_key,NVL(resident_provider_key,NVL(admitting_provider_key,0)))
  LEFT JOIN ref_financial_class fc   ON fc.network = v.network AND fc.financial_class_id = v.financial_class_id
  LEFT JOIN ref_visit_types vt ON vt.visit_type_id  = v.final_visit_type_id
  LEFT JOIN dim_payers pm on pm.payer_key  = v.first_payer_key
  LEFT JOIN patient_secondary_number psn
  ON     psn.network = pp.network
  AND psn.patient_id = pp.patient_id
  AND psn.secondary_nbr_type_id =
       CASE
          WHEN (pp.network = 'GP1' AND f.facility_id = 1) THEN 13
          WHEN (pp.network = 'GP1' AND f.facility_id IN (2, 4)) THEN 11
          WHEN (pp.network = 'GP1' AND f.facility_id = 3) THEN 12
          WHEN (pp.network = 'CBN' AND f.facility_id = 4) THEN 12
          WHEN (pp.network = 'CBN' AND f.facility_id = 5) THEN 13
          WHEN (pp.network = 'NBN' AND f.facility_id = 2) THEN 9
          WHEN (pp.network = 'NBX' AND f.facility_id = 2) THEN 11
          WHEN (pp.network = 'QHN' AND f.facility_id = 2) THEN 11
          WHEN (pp.network = 'SBN' AND f.facility_id = 1) THEN 11
          WHEN (pp.network = 'SMN' AND f.facility_id = 2) THEN 11
          WHEN (pp.network = 'SMN' AND f.facility_id = 7) THEN 13
          WHEN (pp.network = 'SMN' AND f.facility_id = 8) THEN 14
          WHEN (pp.network = 'SMN' AND f.facility_id = 9) THEN 17
       END
)


SELECT /*+  parallel (32) */
 pat.network,
 TO_NUMBER(TO_CHAR(pat.admission_dt, 'YYYYMMDD')) AS admission_dt_key,
 pat.facility_key,
 pat.visit_facility_id AS facility_code,
 pat.visit_facility_name AS facility_name,
 pat.patient_id,
 pat.pat_lname,
 pat.pat_fname,
 pat.mrn,
 pat.birthdate,
 ROUND((pat.admission_dt - pat.birthdate) / 365) AS age,
 pat.apt_suite,
 pat.street_address,
 pat.city,
 pat.state,
 pat.country,
 pat.mailing_code,
 pat.home_phone,
 pat.day_phone,
 pat.pcp,
 pat.visit_id,
 pat.visit_number,
 pat.visit_type_id,
 pat.visit_type,
 pat.admission_dt,
 pat.discharge_dt,
 pat.service AS service_type,
 pat.medicaid_ind,
 pat.payer_group,
 pat.payer_id,
 pat.payer_name,
 pat.plan_id,
 pat.plan_name,
 NVL(diab.diabetes_flag, 0) AS diabetes_flag,
 NVL(dmed.diab_medication_flag, 0) AS diab_medication_flag,
 NVL(kid.kidney_diag_num_flag, 0) AS kidney_diag_num_flag,
 NVL(eye.eye_exam_num_flag, 0) AS eye_exam_num_flag,
 NVL(neph.nephropathy_num_flag, 0) AS nephropathy_num_flag,
 NVL(a1c.hba1c_num_flag, 0) AS hba1c_num_flag,
 NVL(ace.ace_arb_ind, 0) AS ace_arb_ind,
 pat.report_dt
FROM
 denom_pat pat
LEFT JOIN
      (
      SELECT DISTINCT d.network, d.patient_id, 1 as Ace_Arb_ind
      FROM report_dates dt CROSS JOIN fact_patient_prescriptions d
      JOIN ref_drug_descriptions rd
      ON TRIM(rd.drug_description) = TRIM(d.drug_description) AND rd.drug_type_id = 64
      WHERE order_dt >= dt.start_dt  AND order_dt < dt.report_dt
      ) ace on ace.network = pat.network and ace.patient_id  = pat.patient_id
LEFT JOIN
     ( 
      SELECT network, patient_id, diabetes_ind  as diabetes_flag ,
      ROW_NUMBER() OVER (PARTITION BY NETWORK, patient_id ORDER BY admission_dt) diab_cnt
      FROM report_dates dt CROSS JOIN fact_visit_metric_results
      WHERE (admission_dt >=  dt.start_dt AND  admission_dt <  dt.report_dt)
      AND diabetes_ind <> 0
      ) diab ON diab.NETWORK = pat.NETWORK AND diab.patient_id  = pat.patient_id AND diab_cnt = 1
LEFT JOIN
    ( SELECT network, patient_id,  kidney_diseases_ind as kidney_diag_num_flag ,
      ROW_NUMBER() OVER (PARTITION BY NETWORK, patient_id ORDER BY admission_dt) kid_cnt
      FROM report_dates dt CROSS JOIN fact_visit_metric_results
      WHERE (admission_dt >=  dt.start_dt AND  admission_dt <  dt.report_dt)
      AND kidney_diseases_ind <> 0
     )kid  ON kid.NETWORK = pat.NETWORK AND kid.patient_id  = pat.patient_id AND kid_cnt = 1
LEFT JOIN
    (
    SELECT
    network, patient_id, retinal_dil_eye_exam_ind as eye_exam_num_flag , 
    row_number() over (partition by network, patient_id order by admission_dt) eye_cnt
    FROM  report_dates dt CROSS JOIN fact_visit_metric_results
    WHERE (admission_dt >=  dt.start_dt AND  admission_dt <  dt.report_dt)
    AND retinal_dil_eye_exam_ind <> 0
 )eye  ON eye.NETWORK = pat.NETWORK AND eye.patient_id  = pat.patient_id AND eye_cnt = 1
LEFT JOIN
   (
    SELECT
    network, patient_id,  nephropathy_screen_ind  as nephropathy_num_flag , 
    row_number() over (partition by network, patient_id order by admission_dt) neph_cnt
    FROM report_dates dt CROSS JOIN  fact_visit_metric_results
    WHERE (admission_dt >=  dt.start_dt AND  admission_dt <  dt.report_dt)
    AND nephropathy_screen_ind <> 0
 )neph  ON neph.NETWORK = pat.NETWORK AND neph.patient_id  = pat.patient_id AND neph_cnt = 1
LEFT JOIN
  (
  SELECT
  network, patient_id,   decode(NVL(a1c_final_calc_value,0),0,0,1) as hba1c_num_flag , 
  row_number() over (partition by network, patient_id order by admission_dt) a1c_cnt
  FROM report_dates dt CROSS JOIN  fact_visit_metric_results
  WHERE (admission_dt >=  dt.start_dt AND  admission_dt <  dt.report_dt)
  AND a1c_final_calc_value IS NOT NULL
 )a1c  ON a1c.NETWORK = pat.NETWORK AND a1c.patient_id  = pat.patient_id AND a1c_cnt = 1

LEFT JOIN
 (
  SELECT distinct d.network, d.patient_id, 1 AS diab_medication_flag
  FROM report_dates dt 
  CROSS JOIN  fact_patient_prescriptions d
  JOIN ref_drug_descriptions rd
  ON TRIM(rd.drug_description) = TRIM(d.drug_description) AND rd.drug_type_id = 33
  WHERE order_dt >= dt.start_dt  AND order_dt < dt.report_dt
) dmed  ON dmed.NETWORK = pat.NETWORK AND dmed.patient_id  = pat.patient_id 



WHERE
 pat.cnt = 1;


GRANT SELECT ON V_DSRIP_TR_022_DIAB_SCREEN_CDW TO PUBLIC;
/