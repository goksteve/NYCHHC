CREATE OR REPLACE  VIEW V_FACT_VISIT_METRICS
AS
  WITH
  get_dates
   AS
    (
      select 
       TO_NUMBER(TO_CHAR(TRUNC(ADD_MONTHS(SYSDATE, -1), 'MONTH'), 'yyyymmdd') || '000000') AS starting_cid,
     --TO_NUMBER(TO_CHAR(SYSDATE - 10 , 'yyyymmdd') || '000000') AS starting_cid,
     --TRUNC( ADD_MONTHS(SYSDATE, - 12), 'MONTH')   epic_start_dt,
      DATE '2017-01-01' epic_start_dt,
      TRUNC(SYSDATE ) AS start_dt
      from dual
    ),
 crit_metric AS
   (
    SELECT --+ materialize 
    network, criterion_id, VALUE,
    CASE 
    WHEN criterion_id = 13 THEN
    CASE
    WHEN UPPER(value_description) LIKE '%SYS%' THEN 'S' -- systolic
    WHEN UPPER(value_description) LIKE '%DIAS%' THEN 'D' -- diastolic
    ELSE 'C' -- combo
    END
    END  test_type
    FROM meta_conditions
    -- 4-A1C, 10-LDL, 23-Glucose,  13-(incl-15,29)-BP, 66-Neph, 68-eye exam, HOLD 8-HYPERTENSION
    WHERE criterion_id IN (4,10,13,23,66,68) 
   ), 
  rslt AS  --- A1C, LDL, Glucose,   BP (BP only for space)
  (
     SELECT --+ materialize 
      r.network,
      r.visit_id,
      TRIM(r.value) AS result_value,
      c.criterion_id,
      ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY event_id DESC) rnum
      FROM 
     get_dates d
    CROSS JOIN crit_metric c
      JOIN result r  ON r.data_element_id = c.VALUE AND r.network = c.network
       AND  CID >=  d.starting_cid
    --   AND r.event_status_id IN (6, 11)  --  AND r.network =  SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
      WHERE
      TRIM(r.value) IS NOT NULL
      AND c.criterion_id IN (4,10,23,13)
      AND REGEXP_REPLACE
      (
        TRIM(r.value),
      '(([[:digit:]]{1,2})-([[:alpha:]]{2,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{2,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))|
       (([[:alpha:]]{2,9}) ([[:digit:]]{1,2}),([[:digit:]]{2,4}))|(([[:digit:]]{1,2})([[:alpha:]]{3,9})([[:digit:]]{2,4})|([[:digit:]]{4,10})|([^[:digit:]]))'
      )  IS NOT NULL
      AND REGEXP_REPLACE(SUBSTR(TRIM(r.value), 1, 1), '[-\/.,?!$*#^@%)(0&]' ) IS NOT NULL  
      AND NOT REGEXP_LIKE( TRIM(LOWER(r.value)),
      '(^kca)|(^xp)|(^precision)|(^will)|(^smc)|(^mrr)|(^m1)|(^mc)|(^mod)|(^mmr)|(sent)|(pending)|(not)|(unable)|(n/a)|(remind)|(fasting)|(module)|(repeat)|(room)|(floor)|(south)|(north)|(^lkj)|(progress)|(home)')
      AND NOT REGEXP_LIKE(TRIM(LOWER(r.value)),
      '(record)|(patient)|(unable)|(none)|(na)|(arm)|(foot)|(agrees)|(determined)|(note)|(unknown)|(abnormal)|(scanned)|(see)|(proteinuria)|(^m9)|(^m6)|(^m3)|(^m5)|(^m4)|(^m2)|(^s3)|(^s4)|(psyer)|(^kcb)|(^kat)|(^kva)')
       AND NOT REGEXP_LIKE(TRIM(LOWER(r.value)), '(^3n)|(^x\[p)|(^kct)|(over)|(a1c)|(^n9)|(other)|(invalid)')
    UNION ALL
   -- 66-Neph, 68-eye exam,  HOLD-8-HYPERTENSION --- JUST FOR INDICATORS
      SELECT --+ materialize 
      r.network,
      r.visit_id,
      TRIM(r.value) AS result_value,
      c.criterion_id,
      ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY event_id DESC) rnum
      FROM 
      get_dates d
      CROSS JOIN    crit_metric c
      JOIN result r  ON r.data_element_id = c.VALUE AND r.network = c.network and r.CID > =  d.starting_cid
    --  AND r.event_status_id IN (6, 11)  -- AND r.network =  SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
      WHERE
      TRIM(r.value) IS NOT NULL
      AND c.criterion_id  IN (66,68)
),

 bp_rslt AS
   (
    SELECT--+ materialize 
    r.network,
    r.visit_id,
    r.event_id,
    CASE
    WHEN lkp.test_type = 'C' THEN TO_NUMBER(REGEXP_SUBSTR (r.value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',1))
    WHEN lkp.test_type = 'S' THEN TO_NUMBER(REGEXP_SUBSTR (r.value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
    END AS bp_calc_systolic,      
    CASE                          
    WHEN lkp.test_type = 'C' THEN TO_NUMBER(REGEXP_SUBSTR (r.VALUE,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',2))
    WHEN lkp.test_type = 'D' THEN TO_NUMBER(REGEXP_SUBSTR (r.value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
    END  AS bp_calc_diastolic
    FROM 
    get_dates d
    CROSS JOIN crit_metric lkp
    JOIN result r ON r.data_element_id = lkp.value 
    AND r.network = lkp.network  AND lkp.criterion_id = 13 
   -- AND r.event_status_id IN (6, 11)
    AND r.CID >  d.starting_cid
   ),
bp_final_tb
AS
(
SELECT --+ materialize 
    network,
    visit_id,
    TO_CHAR( bp_final_calc_systolic)||'/' ||TO_CHAR( bp_final_calc_diastolic) AS bp_final_calc_value,
    bp_final_calc_systolic  ,
    bp_final_calc_diastolic,
row_number() over (partition by network,visit_id order by  event_id desc) rnum_per_visit
FROM
  (
    SELECT
    r.network,
    r.visit_id,
    r.event_id,
    MAX(bp_calc_systolic) AS bp_final_calc_systolic,
    MAX(bp_calc_diastolic) AS bp_final_calc_diastolic
FROM
 bp_rslt r
      GROUP BY r.network,r.visit_id,r.event_id
     HAVING MAX (bp_calc_systolic) BETWEEN 0 AND 311 AND MAX (bp_calc_diastolic) BETWEEN 0 AND 284
  )
),
calc_result AS
(
  SELECT --+ materialize
  v.network,
  v.visit_id,
  to_number(TO_CHAR( v.admission_dt,'yyyymmdd')) AS admission_dt_key,
  v.visit_number,
  v.facility,
  v.visit_type_id,
  v.visit_type,
  v.medicaid_ind,
  v.medicare_ind,
  v.patient_id,
  v.mrn,
  v.pat_lname,
  v.pat_fname,
  v.sex,
  v.birthdate,
  v.age AS patient_age_at_admission ,
  v.admission_dt,
  v.discharge_dt,
  p.asthma_ind,
  p.bh_ind,
  p.breast_cancer_ind,
  p.diabetes_ind,
  p.heart_failure_ind,
  p.hypertension_ind,
  p.kidney_diseases_ind,
  p.smoker_ind,
  p.pregnancy_ind,
  p.pregnancy_onset_dt,
  p.flu_vaccine_ind,
  p.flu_vaccine_onset_dt, 
  p.pna_vaccine_ind, 
  p.pna_vaccine_onset_dt,
  q.criterion_id,
  q.result_value,
  CASE
      WHEN q.criterion_id IN (10,23) THEN -- Glucose / LDL  <= 1000
        TO_NUMBER
        (
          REGEXP_REPLACE(
          REGEXP_REPLACE (
          REGEXP_REPLACE(
          REGEXP_REPLACE(
          SUBSTR(
          TRIM(
          REGEXP_REPLACE(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.result_value), 
          '(([[:digit:]]{1,2})-([[:alpha:]]{3,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{3,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))|(([[:alpha:]]{2,9}) ([[:digit:]]{1,2}),([[:digit:]]{2,4}))|(([[:digit:]]{1,2})([[:alpha:]]{3,9})([[:digit:]]{2,4}))|([[:digit:]]{4,10})'),
          '([-?!.\=/+,?!><$*#^@%)(&]+$)|(^[-?!.0\=/+,?!><$*#@%)(0&]+)|([[:alpha:]-)(#?!$%])')),
          '^[-?!.0\=/+,?!><$*#@%]'),
          '([:*&%$;=>/`])|(\.+$)')
          ), 1, 4)
          , '[^[:digit:].,]'), ',','.'), '\.+$'), '(\d)(\.)(\.)(\d)', '\1.\4')
        )
     WHEN q.criterion_id = 4 THEN --  A1C < 50
      TO_NUMBER
      (
        REGEXP_REPLACE(
        REGEXP_REPLACE(
        REGEXP_REPLACE(
        REGEXP_REPLACE(
        SUBSTR(
        TRIM(
        REGEXP_REPLACE(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(q.result_value), 
        '(([[:digit:]]{1,2})-([[:alpha:]]{3,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{3,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))|(([[:alpha:]]{2,9}) ([[:digit:]]{1,2}),([[:digit:]]{2,4}))|(([[:digit:]]{1,2})([[:alpha:]]{3,9})([[:digit:]]{2,4}))|([[:digit:]]{4,10})'),
        '([-?!.\=/+,?!><$*#^@%)(&]+$)|(^[-?!.0\=/+,?!><$*#@%)(0&]+)|([[:alpha:]-)(#?!$%])')),
        '^[-?!.0\=/+,?!><$*#@%]'),
        '([:*&%$;=>/`])|(\.+$)')
        ), 1, 4)
        , '[^[:digit:].,]'), ',','.'), '\.+$'), '(\d)(\.)(\.)(\d)', '\1.\4')
      )
    WHEN q.criterion_id = 13 THEN --BP
        0
    WHEN q.criterion_id IN (66,68) THEN -- NEPH,eye EXAM
        1
    END  AS calc_value
    FROM
    STG_METRICS_DAILY_VISITS v 
    LEFT JOIN  fact_patient_metric_diag p ON p.patient_id = v.patient_id AND p.network = v.network  
    LEFT  JOIN rslt q ON q.visit_id = v.visit_id AND q.network = v.network AND q.rnum = 1
 ),
final_calc_tb
AS
(
  SELECT --+ materialize
  network,
  visit_id,
  admission_dt_key,
  visit_number,
  facility,
  visit_type_id,
  visit_type,
  medicaid_ind,
  medicare_ind,
  patient_id,
  mrn,
  pat_lname,
  pat_fname,
  sex,
  birthdate,
  patient_age_at_admission ,
  admission_dt,
  discharge_dt,
  asthma_ind,
  bh_ind,
  breast_cancer_ind,
  diabetes_ind,
  heart_failure_ind,
  hypertension_ind,
  kidney_diseases_ind,
  smoker_ind,
  pregnancy_ind,
  pregnancy_onset_dt,
  flu_vaccine_ind,
  flu_vaccine_onset_dt, 
  pna_vaccine_ind, 
  pna_vaccine_onset_dt,
  neph_final_calc_value,
  retinal_final_calc_value,
  a1c_final_calc_value,
  gluc_final_calc_value,
  ldl_final_calc_value,
  bp_final_calc_value
  FROM
   calc_result
    PIVOT
     ( 
       MAX(result_value) AS final_orig_value, 
       MAX(calc_value) AS final_calc_value
      FOR criterion_id
     IN (4 AS a1c, 23 AS gluc, 10 AS ldl, 13 AS bp, 66 as neph, 68 as retinal ))
 ),
QCPR_TMP
AS
(
SELECT --+  materialize
a. network,
a.visit_id,
admission_dt_key,
visit_number,
facility,
visit_type_id,
visit_type,
medicaid_ind,
medicare_ind,
patient_id,
mrn,
pat_lname,
pat_fname,
sex,
birthdate,
patient_age_at_admission ,
admission_dt,
discharge_dt,
NVL(a.asthma_ind,0) asthma_ind,
NVL(a.bh_ind,0) bh_ind,
NVL(a.breast_cancer_ind,0) breast_cancer_ind,
NVL(a.diabetes_ind,0) diabetes_ind,
NVL(a.heart_failure_ind,0) heart_failure_ind,
NVL(a.hypertension_ind, 0) hypertension_ind,
NVL(a.kidney_diseases_ind,0) kidney_diseases_ind,
NVL(a.smoker_ind,0) smoker_ind,
NVL( pregnancy_ind,0) AS pregnancy_ind,
pregnancy_onset_dt,
NVL(flu_vaccine_ind,0) AS flu_vaccine_ind,
flu_vaccine_onset_dt,
NVL(pna_vaccine_ind,0 )AS pna_vaccine_ind,
pna_vaccine_onset_dt,
NVL( a.neph_final_calc_value,0) as nephropathy_screen_ind,
NVL( a.retinal_final_calc_value,0) as retinal_dil_eye_exam_ind,
CASE WHEN a.a1c_final_calc_value > 50 THEN 0 ELSE a.a1c_final_calc_value END AS  a1c_final_calc_value,
CASE WHEN a.gluc_final_calc_value > 999 THEN  0 ELSE a.gluc_final_calc_value END AS gluc_final_calc_value  ,
CASE WHEN  a.ldl_final_calc_value  > 999 THEN 0 ELSE  a.ldl_final_calc_value END  AS ldl_final_calc_value,
NVL(b.bp_final_calc_value, a.bp_final_calc_value) AS bp_final_calc_value,
b.bp_final_calc_systolic,
b.bp_final_calc_diastolic
FROM
 final_calc_tb a
LEFT JOIN bp_final_tb b ON b.network = a.network AND b.visit_id = a.visit_id AND b.rnum_per_visit = 1)

SELECT 
 a.network,
 NVL( v.visit_key,999999999999) as visit_key,
 a.visit_id,
NVL( p.patient_key,999999999999) as patient_key,
 a.admission_dt_key,
 a.visit_number,
 a.facility,
 a.visit_type_id,
 vt.name as visit_type,
 a.medicaid_ind,
 a.medicare_ind,
 CAST(a.patient_id AS VARCHAR2(256)) patient_id,
 a.mrn,
 a.pat_lname || ', ' || a.pat_fname AS patient_name,
 a.sex,
 race_desc AS race,
 TRUNC(a.birthdate) AS birthdate,
 a.patient_age_at_admission,
 TRUNC(a.admission_dt) AS admission_dt,
 TRUNC(a.discharge_dt) AS discharge_dt,
 asthma_ind,
 bh_ind,
 breast_cancer_ind,
 diabetes_ind,
 heart_failure_ind,
 hypertension_ind,
 kidney_diseases_ind,
 smoker_ind,
 pregnancy_ind,
 pregnancy_onset_dt,
  flu_vaccine_ind,
  flu_vaccine_onset_dt, 
  pna_vaccine_ind, 
  pna_vaccine_onset_dt,
 nephropathy_screen_ind,
 retinal_dil_eye_exam_ind,
 a1c_final_calc_value,
 gluc_final_calc_value,
 ldl_final_calc_value,
 bp_final_calc_value,
 bp_final_calc_systolic,
 bp_final_calc_diastolic,
 'QCPR' AS source ,
 trunc(sysdate) as load_dt
FROM
 qcpr_tmp a
 LEFT JOIN DIM_PATIENTS p on p.network = a.network and p.patient_id  = a.patient_id and p.current_flag  = 1
 LEFT JOIN fact_visits v ON  v.NETWORK = a.NETWORK AND v.visit_id  =  a.visit_id
 LEFT JOIN REF_VISIT_TYPES vt on vt.visit_type_id = a.visit_type_id
WHERE
 a.admission_dt < TRUNC(SYSDATE)
UNION ALL
SELECT
 DISTINCT network,
          999999999999 as visit_key,
          visit_id,
          patient_key,
          TO_NUMBER(TO_CHAR(admission_dt, 'yyyymmdd')) AS admission_dt_key,
          NULL visit_number,
          -- facility_key,
          facility_name,
          NULL visit_type_id,
          visit_type,
          NULL medicaid_ind,
          NULL as medicare_ind,
          patient_id,
          mrn,
          patient_name,
          sex,
          NULL AS race,
          birthdate,
          patient_age_at_admission,
          admission_dt,
          discharge_dt,
          asthma_ind,
          bh_ind,
          breast_cancer_ind,
          diabetes_ind,
          heart_failure_ind,
          hypertension_ind,
          kidney_diseases_ind,
          smoker_ind,
          pregnancy_ind,
          TRUNC(pregnancy_onset_dt),
          flu_vaccine_ind,
          flu_vaccine_onset_dt, 
          pna_vaccine_ind, 
          pna_vaccine_onset_dt,
          nephropathy_screen_ind,
          retinal_eye_exam_ind AS retinal_dil_eye_exam_ind,
          a1c_value,
          NULL gluc_final_calc_value,
          ldl_calc_value,
          bp_orig_value,
          bp_systolic,
          bp_diastolic,
          'EPIC' AS source,
           trunc(sysdate) as load_dt
FROM
 get_dates 
CROSS JOIN  STG_VISIT_METRICS_EPIC
WHERE   admission_dt >= epic_start_dt AND admission_dt < TRUNC(SYSDATE);


GRANT SELECT ON v_fact_visit_metrics TO PUBLIC;
/
