CREATE OR REPLACE VIEW V_DSRIP_TR026_APD_CDW AS
  WITH report_dates AS
--Denominator
--1) Number of people, ages 19 to 64 years, 
--2) having schizophrenia (See "Schizo_Dementia ICD Codes" tab) and met at least one of the following criteria during both the measurement year:
--3)  At least one  visit outpatient , inpatient encounter with a any diagnosis of schizophrenia (See "Schizo_Dementia ICD Codes" tab)
--********************************
--Exclude patients meeting at least one of the following criteria during the measurement year:
--1. Patients who had a diagnosis of dementia (see "Schizo_Dementia ICD Codes" tab)
--2. Patients who did not have at least two antipsychotic medication dispensing events (see "Oral Meds with Formulas" or "Injectable Meds with Formulas" tabs)
--********************************
--108- MEDICATIONS:ANTIPSYCHOTIC INJECTABLE MEDICATION 14 DAYS	List of Antipsychotic Injectable medications 14 days cover 
--107-MEDICATIONS:ANTIPSYCHOTIC INJECTABLE MEDICATION 28 DAYS	List of Antipsychotic Injectable medications 28 days cover 
--106-MEDICATIONS:ANTIPSYCHOTIC ORAL MEDICATION	List of Antipsychotic oral medications
--105 - DIAGNOSES:DEMENTIA	List of  Dimentia Diagnoses
--31 - DIAGNOSES:SCHIZOPHRENIA




    (
      SELECT --+ materialize
      TRUNC(SYSDATE, 'MONTH') report_dt,
      ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) start_dt,
      ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) rslt_start_date,
      ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
      FROM DUAL
    ),
  all_pat_diag 
  AS
    (
    SELECT --+  materialize 
    DISTINCT
    d.network, d.patient_id, mc.criterion_id as crit_id, onset_date,
    row_number() over (partition by  d.network, d.patient_id, mc.criterion_id  order by onset_date DESC) cnt
    FROM  report_dates CROSS JOIN  meta_conditions mc JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE
    WHERE
    mc.criterion_id IN (31,105 )AND d.status_id IN (0, 6, 7,8)
    AND   onset_date  < report_dt
    ) ,

 denom_visit  --31 has - DIAGNOSES:SCHIZOPHRENIA
     AS
     (
       SELECT --+  materialize
       d.network, d.patient_id,visit_id, diagnosis_dt, icd_code,PROBLEM_COMMENTS,facility_key,report_dt ,
       row_number() over (partition by  d.network, d.patient_id order by diagnosis_dt DESC) v_cnt
       FROM report_dates CROSS JOIN    meta_conditions mc JOIN fact_visit_diagnoses d ON d.icd_code = mc.VALUE
       WHERE  mc.criterion_id IN (31)  
          AND   diagnosis_dt >= start_dt AND diagnosis_dt < report_dt
      ) ,
 denom_pat
  AS(
     (
      SELECT  network, patient_id
      FROM  all_pat_diag  WHERE crit_id =  31 AND cnt = 1
     UNION 
      SELECT  network, patient_id
      FROM  denom_visit where v_cnt  = 1
      )
    MINUS
    SELECT  network, patient_id
    FROM  all_pat_diag  WHERE crit_id =  105 AND cnt = 1
    ),
tmp_prescr AS
(
  SELECT  /*+ parallel (32 ) */
    network,
    patient_id,
    order_dt as ,
    drug_description,
    dosage,
    frequency,
    daily_cnt,
    rx_quantity,
    rx_refills,
    SUM( NVL(rx_refills, 0) )  over ( partition by network,   patient_id)as  total_refils,
    (SUM( NVL(rx_refills, 0)+1 )  over ( partition by network,   patient_id)* rx_quantity /daily_cnt) days_covered,
    Row_number() over ( partition by network,   patient_id order by order_dt ) drug_cnt
  FROM
      (
        SELECT distinct
          d.network,
          d.patient_id,
          d.order_dt,
          d.drug_description,
          d.dosage,
          d.frequency,
          NVL( a.drug_frequency_num_val,1) as daily_cnt,
          d.rx_quantity,
          d.rx_refills,
          COUNT(*) OVER ( PARTITION BY d.NETWORK, d.PATIENT_ID ) AS dispens_CNT,
         dt.report_dt
        FROM
        report_dates dt
        CROSS JOIN
        fact_patient_prescriptions d
        JOIN ref_drug_descriptions rd  ON rd.drug_description = d.drug_description AND rd.drug_type_id IN (106, 107, 108)
        JOIN denom_pat pd on pd.network = d.network and pd.patient_id  = d.patient_id
        LEFT JOIN ref_drug_frequency a  ON d.frequency LIKE a.drug_frequency
        WHERE  d.order_dt >= dt.start_dt AND d.order_dt < dt.report_dt
      )
  WHERE dispens_CNT > 1
),
final_denom
AS
 (
    SELECT  --+ materialize
    tp.network,
    tp.patient_id,
    tp.order_dt AS earliest_prescribed_dt,
    tp.drug_description,
    days_covered,
    total_refils,
    report_dt - tp.order_dt AS treatment_period,
    CASE
    WHEN ROUND(days_covered / (report_dt - tp.order_dt), 1) >= 1 THEN 100
    ELSE ROUND(days_covered / (report_dt - tp.order_dt), 2) * 100
    END
    AS pdc_ratio,
    CASE WHEN ROUND(days_covered / (report_dt - tp.order_dt), 1) * 100 >= 80 THEN 1 ELSE 0 END AS numerator_flag,
    dv.visit_id,
    dv.diagnosis_dt,
    dv.icd_code,
    dv.problem_comments,
    dv.facility_key,
    dv.report_dt
    FROM
    tmp_prescr tp 
    JOIN denom_visit dv ON dv.network = tp.network AND dv.patient_id = tp.patient_id AND v_cnt = 1
    WHERE
    drug_cnt = 1
    )
    select --+ parallel(32)
    d.network,
    d.patient_id,
    f.facility_id AS  facility_id,
    f.facility_name AS facility_name,
    SUBSTR(pp.name, 1, INSTR(pp.name, ',', 1) - 1) AS pat_lname,
    SUBSTR(pp.name, INSTR(pp.name, ',') + 1) AS pat_fname,
    NVL(psn.second_mrn, pp.medical_record_number) AS mrn,
    pp.birthdate,
    ROUND((v.admission_dt - pp.birthdate) / 365) AS age,
    pp.apt_suite,
    pp.street_address,
    pp.city,
    pp.state,
    pp.country,
    pp.mailing_code,
    pp.home_phone,
    pp.day_phone,
    v.final_visit_type_id AS visit_type_id,
    vt.name AS visit_type,
    v.admission_dt,
    v.discharge_dt,
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
    v.financial_class_id AS plan_id,
    fc.financial_class_name AS plan_name,
    d.earliest_prescribed_dt,
    d.drug_description,
    case when d.days_covered > treatment_period then treatment_period ELSE d.days_covered END AS days_covered,
    d.total_refils,
    d.treatment_period,
    d.pdc_ratio,
    d.numerator_flag,
    'DSRIP_TR026' AS dsrip_report,
    d.report_dt,
    TRUNC(SYSDATE) AS load_dt
 FROM final_denom d
 JOIN fact_visits v ON v.NETWORK  = d.NETWORK AND v.visit_id  = d.visit_id
 JOIN dim_patients pp ON pp.NETWORK  = d.NETWORK AND pp.patient_id = d.patient_id AND pp.current_flag  = 1
 LEFT JOIN dim_hc_facilities f   ON f.facility_key = NVL(d.facility_key, v.facility_key)
 LEFT JOIN ref_visit_types vt ON vt.visit_type_id  = v.final_visit_type_id
  LEFT JOIN dim_payers pm ON pm.payer_key  = v.first_payer_key
 LEFT JOIN ref_financial_class fc   ON fc.NETWORK = v.NETWORK AND fc.financial_class_id = v.financial_class_id
 LEFT JOIN ref_patient_secondary_mrn psn  ON   psn.NETWORK = d.NETWORK AND psn.patient_id = d.patient_id AND psn.facility_key = NVL(d.facility_key, v.facility_key)

WHERE 1=1
 AND FLOOR((v.admission_dt - pp.birthdate) / 365) BETWEEN 19 AND 64


