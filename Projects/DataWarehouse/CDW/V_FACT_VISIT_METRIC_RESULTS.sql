CREATE OR REPLACE VIEW v_fact_visit_metric_results AS

-- 2018-April-10 SG OK GK OK --
-- 2018-April-18 SG UPDATED BY SG 
-- 2018-April-25 SG UPDATED BY SG 
-- 2018-MAY-2 SG UPDATED added some ind BY SG 
-- 2018-MAY-16 add pregnancy ind SG
-- 2018-JUNE-16 add  p.flu_vaccine_ind, SG
  WITH crit_metric AS
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
    WHERE criterion_id IN (4,10,23,13,66,68)   ), -- A1C, LDL, Glucose,  BP, Neph, eye eaxm

  rslt AS  --- A1C, LDL, Glucose,  BP only
  (
     SELECT --+ materialize
      r.network,
      r.visit_id,
      r.patient_key,
      r.patient_id,
      result_dt,
      TRIM(r.result_value) AS result_value,
      c.criterion_id,
      ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY result_dt DESC) rnum
      FROM    crit_metric c
      JOIN fact_results r  ON r.data_element_id = c.VALUE AND r.network = c.network
      AND r.event_status_id IN (6, 11)   AND r.network =  SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
      WHERE
      TRIM(r.result_value) IS NOT NULL
      AND c.criterion_id IN (4,10,23,13)
      AND REGEXP_REPLACE
      (
        TRIM(r.result_value),
      '(([[:digit:]]{1,2})-([[:alpha:]]{2,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{2,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))|
       (([[:alpha:]]{2,9}) ([[:digit:]]{1,2}),([[:digit:]]{2,4}))|(([[:digit:]]{1,2})([[:alpha:]]{3,9})([[:digit:]]{2,4})|([[:digit:]]{4,10})|([^[:digit:]]))'
      )  IS NOT NULL
      AND REGEXP_REPLACE(SUBSTR(TRIM(r.result_value), 1, 1), '[-\/.,?!$*#^@%)(0&]' ) IS NOT NULL  
      AND NOT REGEXP_LIKE( TRIM(LOWER(r.result_value)),
      '(^kca)|(^xp)|(^precision)|(^will)|(^smc)|(^mrr)|(^m1)|(^mc)|(^mod)|(^mmr)|(sent)|(pending)|(not)|(unable)|(n/a)|(remind)|(fasting)|(module)|(repeat)|(room)|(floor)|(south)|(north)|(^lkj)|(progress)|(home)')
      AND NOT REGEXP_LIKE(TRIM(LOWER(r.result_value)),
      '(record)|(patient)|(unable)|(none)|(na)|(arm)|(foot)|(agrees)|(determined)|(note)|(unknown)|(abnormal)|(scanned)|(see)|(proteinuria)|(^m9)|(^m6)|(^m3)|(^m5)|(^m4)|(^m2)|(^s3)|(^s4)|(psyer)|(^kcb)|(^kat)|(^kva)')
       AND NOT REGEXP_LIKE(TRIM(LOWER(r.result_value)), '(^3n)|(^x\[p)|(^kct)|(over)|(a1c)|(^n9)|(other)|(invalid)')
    UNION ALL
      SELECT --+ materialize
      r.network,
      r.visit_id,
      r.patient_key,
      r.patient_id,
      result_dt,
      result_value,
      c.criterion_id,
      ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY result_dt DESC) rnum
      FROM    crit_metric c
      JOIN fact_results r  ON r.data_element_id = c.VALUE AND r.network = c.network
      AND r.event_status_id IN (6, 11)   AND r.network =  SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
      WHERE
      TRIM(r.result_value) IS NOT NULL
      AND c.criterion_id  IN (66,68)
   ),
 bp_rslt AS
   (
    SELECT--+ materialize 
    r.network,
    r.visit_id,
    r.event_id,
    r.result_dt,
    CASE
    WHEN lkp.test_type = 'C' THEN TO_NUMBER(REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',1))
    WHEN lkp.test_type = 'S' THEN TO_NUMBER(REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
    END AS bp_calc_systolic,      
    CASE                          
    WHEN lkp.test_type = 'C' THEN TO_NUMBER(REGEXP_SUBSTR (r.result_VALUE,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',2))
    WHEN lkp.test_type = 'D' THEN TO_NUMBER(REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
    END  AS bp_calc_diastolic
    FROM crit_metric lkp
    JOIN fact_results r ON r.data_element_id = lkp.value 
    AND r.network = lkp.network  AND lkp.criterion_id = 13 
    AND r.event_status_id IN (6, 11)
    AND r.network =  SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
   ),
bp_final_tb
AS
(
SELECT --+ materialize 
    network,
    visit_id,
    result_dt AS bp_final_result_dt,
   TO_CHAR( bp_final_calc_systolic)||'/' ||TO_CHAR( bp_final_calc_diastolic) AS bp_final_calc_value,
    bp_final_calc_systolic  ,
    bp_final_calc_diastolic,
row_number() over (partition by network,visit_id order by result_dt desc) rnum_per_visit
FROM
  (
    SELECT
    r.network,
    r.visit_id,
    r.event_id,
    trunc(r.result_dt) as result_dt ,
    MAX(bp_calc_systolic) AS bp_final_calc_systolic,
    MAX(bp_calc_diastolic) AS bp_final_calc_diastolic
FROM
 bp_rslt r
      GROUP BY r.network,r.visit_id,r.event_id,r.result_dt
     HAVING MAX (bp_calc_systolic) BETWEEN 0 AND 311 AND MAX (bp_calc_diastolic) BETWEEN 0 AND 284
  )
),
calc_result AS
(
SELECT --+ materialize
 v.network,
 v.visit_id,
 v.visit_key,
 v.patient_key,
 v.facility_key,
 v.admission_dt_key,
 v.discharge_dt_key,
 v.visit_number,
 v.patient_id,
 v.admission_dt,
 v.discharge_dt,
 v.patient_age_at_admission,
 v.first_payer_key,
 v.initial_visit_type_id,
 v.final_visit_type_id,
 p.asthma_ind,
 p.bh_ind,
 p.breast_cancer_ind,
 p.diabetes_ind,
 p.heart_failure_ind,
 p.hypertension_ind,
 p.kidney_diseases_ind,
 p.smoker_ind,
 p.pregnancy_ind,
 pregnancy_onset_dt,
 p.flu_vaccine_ind,
 p.flu_vaccine_onset_dt, 
 p.pna_vaccine_ind, 
 p.pna_vaccine_onset_dt,
 p.bronchitis_ind,	
 p.bronchitis_onset_dt,		
 q.criterion_id,
 TRUNC(q.result_dt) AS result_dt,
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
  fact_visits v 
  LEFT JOIN  fact_patient_metric_diag p ON p.patient_id = v.patient_id AND p.network = v.network  
  LEFT  JOIN rslt q ON q.visit_id = v.visit_id AND q.network = v.network AND q.rnum = 1
  WHERE  v.network =   SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
 AND Admission_dt >= DATE '2014-01-01'
),
final_calc_tb
AS
(
    SELECT --+ materialize
    network,
    visit_id,
    patient_key,
    facility_key,
    admission_dt_key,
    discharge_dt_key,
    visit_number,
    patient_id,
    admission_dt,
    discharge_dt,
    patient_age_at_admission,
    first_payer_key,
    initial_visit_type_id,
    final_visit_type_id,
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
    bronchitis_ind,	
    bronchitis_onset_dt,	
    neph_final_result_dt,
    neph_final_orig_value,
    neph_final_calc_value,
    retinal_final_result_dt,
    retinal_final_orig_value,
    retinal_final_calc_value,
    a1c_final_result_dt,
    a1c_final_orig_value,
    a1c_final_calc_value,
    gluc_final_result_dt,
    gluc_final_orig_value,
    gluc_final_calc_value,
    ldl_final_result_dt,
    ldl_final_orig_value,
    ldl_final_calc_value,
    bp_final_result_dt,
    bp_final_orig_value,
    bp_final_calc_value
FROM
 calc_result
  PIVOT
   ( MAX(result_dt) AS final_result_dt,
     MAX(result_value) AS final_orig_value, 
     MAX(calc_value) AS final_calc_value
   FOR criterion_id
   IN (4 AS a1c, 23 AS gluc, 10 AS ldl, 13 AS bp, 66 as neph, 68 as retinal ))
)


SELECT --+  PARALLEL (48)
 a.network,
 a.visit_id,
 a.patient_key,
 a.facility_key,
 a.admission_dt_key,
 a.discharge_dt_key,
 a.visit_number,
 a.patient_id,
 a.admission_dt,
 a.discharge_dt,
 a.patient_age_at_admission,
 a.first_payer_key,
 a.initial_visit_type_id,
 a.final_visit_type_id,
 NVL(a.asthma_ind, 0) asthma_ind,
 NVL(a.bh_ind, 0) bh_ind,
 NVL(a.breast_cancer_ind, 0) breast_cancer_ind,
 NVL(a.diabetes_ind, 0) diabetes_ind,
 NVL(a.heart_failure_ind, 0) heart_failure_ind,
 NVL(a.hypertension_ind, 0) hypertansion_ind,
 NVL(a.kidney_diseases_ind, 0) kidney_diseases_ind,
 NVL(smoker_ind, 0) AS smoker_ind,
 NVL(pregnancy_ind, 0) AS pregnancy_ind,
 pregnancy_onset_dt,
 NVL(flu_vaccine_ind, 0) AS flu_vaccine_ind,
 flu_vaccine_onset_dt,
 NVL(pna_vaccine_ind, 0) AS pna_vaccine_ind,
 pna_vaccine_onset_dt,
 NVL(bronchitis_ind, 0) AS bronchitis_ind,
 bronchitis_onset_dt,
 NVL(a.neph_final_calc_value, 0) AS nephropathy_screen_ind,
 NVL(a.retinal_final_calc_value, 0) AS retinal_dil_eye_exam_ind,
 a.a1c_final_result_dt,
 CASE WHEN a.a1c_final_calc_value between  3.5 and 22  THEN a.a1c_final_calc_value  ELSE 0 END AS a1c_final_calc_value,
 a.gluc_final_result_dt,
 CASE WHEN a.gluc_final_calc_value > 999 THEN 0 ELSE a.gluc_final_calc_value END AS gluc_final_calc_value,
 a.ldl_final_result_dt,
 CASE WHEN a.ldl_final_calc_value > 999 THEN 0 ELSE a.ldl_final_calc_value END AS ldl_final_calc_value,
 NVL(b.bp_final_result_dt, a.bp_final_result_dt) AS bp_final_result_dt,
 NVL(b.bp_final_calc_value, a.bp_final_calc_value) AS bp_final_calc_value,
 b.bp_final_calc_systolic,
 b.bp_final_calc_diastolic
FROM
 final_calc_tb a
 LEFT JOIN bp_final_tb b ON b.network = a.network AND b.visit_id = a.visit_id AND b.rnum_per_visit = 1
WHERE
 NVL(a.asthma_ind, 0) <> 0
 OR NVL(a.bh_ind, 0) <> 0
 OR NVL(a.breast_cancer_ind, 0) <> 0
 OR NVL(a.diabetes_ind, 0) <> 0
 OR NVL(a.heart_failure_ind, 0) <> 0
 OR NVL(a.hypertension_ind, 0) <> 0
 OR NVL(a.kidney_diseases_ind, 0) <> 0
 OR NVL(smoker_ind, 0) <> 0
 OR NVL(pregnancy_ind, 0) <> 0
 OR NVL(flu_vaccine_ind, 0) <> 0
 OR NVL(pna_vaccine_ind, 0) <> 0
 OR NVL(bronchitis_ind, 0) <> 0
 OR NVL(a.neph_final_calc_value, 0) <> 0
 OR NVL(a.retinal_final_calc_value, 0) <> 0
 OR a.a1c_final_orig_value IS NOT NULL
 OR a.gluc_final_orig_value IS NOT NULL
 OR a.ldl_final_orig_value IS NOT NULL
 OR a.neph_final_orig_value IS NOT NULL
 OR a.retinal_final_orig_value IS NOT NULL
 OR NVL(b.bp_final_calc_value, a.bp_final_calc_value) IS NOT NULL