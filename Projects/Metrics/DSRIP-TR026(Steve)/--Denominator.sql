--Denominator
--1) Number of people, ages 19 to 64 years, 
--2) having schizophrenia (See "Schizo_Dementia ICD Codes" tab) and met at least one of the following criteria during both the measurement year:
--3)  At least one  visit outpatient , inpatient encounter with a any diagnosis of schizophrenia (See "Schizo_Dementia ICD Codes" tab)


--Exclue patients meeting at least one of the following criteria during the measurement year:
--1. Patients who had a diagnosis of dementia (see "Schizo_Dementia ICD Codes" tab)
--2. Patients who did not have at least two antipsychotic medication dispensing events (see "Oral Meds with Formulas" or "Injectable Meds with Formulas" tabs)
-----------------------------------------
--108- MEDICATIONS:ANTIPSYCHOTIC INJECTABLE MEDICATION 14 DAYS	List of Antipsychotic Injectable medications 14 days cover 
--107-MEDICATIONS:ANTIPSYCHOTIC INJECTABLE MEDICATION 28 DAYS	List of Antipsychotic Injectable medications 28 days cover 
--106-MEDICATIONS:ANTIPSYCHOTIC ORAL MEDICATION	List of Antipsychotic oral medications
--105 - DIAGNOSES:DEMENTIA	List of  Dimentia Diagnoses
--31 - DIAGNOSES:SCHIZOPHRENIA

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
pat_diag  --31 have - DIAGNOSES:SCHIZOPHRENIA  
AS
(
  SELECT --+  materialize
  d.network, d.patient_id,  criterion_id AS crit_id, onset_date
  FROM    meta_conditions mc JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE
  WHERE
  mc.criterion_id IN (31)AND d.status_id IN (0, 6, 7,8)
 ) ,

visit_pat
AS
(
SELECT
 v.*,
  row_number() over ( partition by  v.network, v.patient_id  order by  v.admission_dt desc) cnt,
 dt.*
FROM
 report_dates dt
 CROSS JOIN meta_conditions mc
 JOIN fact_visit_diagnoses dv  ON dv.icd_code = mc.VALUE
 JOIN fact_visits v   ON v.network = dv.network    AND v.visit_id = dv.visit_id      AND (v.admission_dt >= dt.start_dt AND v.admission_dt < dt.report_dt)
 WHERE   mc.criterion_id IN (31)


),
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
   NVL(sec.second_mrn, pp.medical_record_number) AS mrn,
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
    v.last_department_key,
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
  LEFT JOIN ref_patient_secondary_mrn sec ON sec.network = pp.network    AND sec.patient_id = pp.patient_id  AND sec.facility_key = v.facility_key
)
