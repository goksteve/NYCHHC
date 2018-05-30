DROP MATERIALIZED VIEW mv_fact_daily_visits_stats;
--Created 20-May -2018 by SG
--SELECT * FROM user_JOBS
CREATE MATERIALIZED VIEW mv_fact_daily_visits_stats
 BUILD IMMEDIATE
 REFRESH
  COMPLETE
  START WITH TRUNC(SYSDATE) + 23 / 24
  NEXT (TRUNC(SYSDATE) + 1) + 23 / 24 AS



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
      TRIM(r.value) AS result_value,
      c.criterion_id,
      ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY event_id DESC) rnum
      FROM    crit_metric c
      JOIN result r  ON r.data_element_id = c.VALUE AND r.network = c.network
       AND  CID > to_number(to_char(sysdate -11,'yyyymmdd')||'000000')
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
      SELECT --+ materialize 
      r.network,
      r.visit_id,
      TRIM(r.value) AS result_value,
      c.criterion_id,
      ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY event_id DESC) rnum
      FROM    crit_metric c
      JOIN result r  ON r.data_element_id = c.VALUE AND r.network = c.network and CID >  to_number(to_char(sysdate -11,'yyyymmdd')||'000000')
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
    FROM crit_metric lkp
    JOIN result r ON r.data_element_id = lkp.value 
    AND r.network = lkp.network  AND lkp.criterion_id = 13 
   -- AND r.event_status_id IN (6, 11)
    AND CID > to_number(to_char(sysdate -11,'yyyymmdd')||'000000')
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
  p.hypertansion_ind,
  p.kidney_diseases_ind,
  pregnancy_ind,
  pregnancy_onset_dt,
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
    FACT_DAILY_VISITS_STATS v 
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
  hypertansion_ind,
  kidney_diseases_ind,
  pregnancy_ind,
  pregnancy_onset_dt,
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
NVL(a.hypertansion_ind, 0) hypertansion_ind,
NVL(a.kidney_diseases_ind,0) kidney_diseases_ind,
NVL( pregnancy_ind,0) AS pregnancy_ind,
pregnancy_onset_dt,
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

SELECT --+PARALLEL (48)
 a.network,
 a.visit_id,
 a.admission_dt_key,
 a.visit_number,
 a.facility,
 a.visit_type_id,
 a.visit_type,
 a.medicaid_ind,
 CAST(a.patient_id AS VARCHAR2(256)) patient_id,
 mrn,
 a.pat_lname || ', ' || a.pat_fname AS patient_name,
 sex,
 TRUNC(a.birthdate) AS birthdate,
 patient_age_at_admission,
 TRUNC(a.admission_dt) AS admission_dt,
 TRUNC(a.discharge_dt) AS discharge_dt,
 asthma_ind,
 bh_ind,
 breast_cancer_ind,
 diabetes_ind,
 heart_failure_ind,
 hypertansion_ind,
 kidney_diseases_ind,
 pregnancy_ind,
 pregnancy_onset_dt,
 nephropathy_screen_ind,
 retinal_dil_eye_exam_ind,
 a1c_final_calc_value,
 gluc_final_calc_value,
 ldl_final_calc_value,
 bp_final_calc_value,
 bp_final_calc_systolic,
 bp_final_calc_diastolic,
 'QCPR' AS source,
 trunc(sysdate) as load_dt
FROM
 qcpr_tmp a
WHERE
 admission_dt < TRUNC(SYSDATE)
UNION
SELECT
 network,
 visit_id,
 TO_NUMBER(TO_CHAR(admission_dt, 'yyyymmdd')) AS admission_dt_key,
 NULL visit_number,
 -- facility_key,
 facility_name,
 NULL visit_type_id,
 visit_type,
 NULL medicaid_ind,
 patient_id,
 mrn,
 patient_name,
 sex,
 TRUNC(CAST(birth_date AS DATE)) birth_date,
 age,
 TRUNC(CAST(admission_dt AS DATE)) admission_dt,
 TRUNC(CAST(discharge_dt AS DATE)) discharge_dt,
 asthma_ind,
 bh_ind,
 breast_cancer_ind,
 diabetes_ind,
 heart_failure_ind,
 hypertansion_ind,
 kidney_diseases_ind,
 pregnancy_ind,
 TRUNC(pregnancy_onset_dt),
 nephropathy_screen_ind,
 retinal_eye_exam_ind  AS  retinal_dil_eye_exam_ind,
 NULL a1c_final_calc_value,
 NULL gluc_final_calc_value,
 NULL ldl_final_calc_value,
 NULL bp_final_calc_value,
 NULL bp_final_calc_systolic,
 NULL bp_final_calc_diastolic,
 'EPIC' AS source,
 trunc(sysdate) as load_dt
FROM
 epic_fact_daily_visits_stats;


CREATE INDEX idx_mv_fact_daily_visits_stats
 ON mv_fact_daily_visits_stats(network, visit_id, source)
 LOGGING;

GRANT SELECT ON mv_fact_daily_visits_stats TO PUBLIC