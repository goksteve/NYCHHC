CREATE OR REPLACE VIEW v_fact_visit_metric_results AS
-- 2018-April-10 SG OK GK OK --
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
    WHERE criterion_id IN (4,10,23,13)
   ), -- A1C, LDL, Glucose,  BP,
      rslt AS
   (
    SELECT --+ materialize 
    r.network,
    r.visit_id,
    r.patient_key,
    r.patient_id,
    result_dt,
    TRIM(r.result_value)  AS result_value,
    c.criterion_id,
    ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY result_dt DESC) rnum
    FROM
    crit_metric c
    JOIN fact_results r
    ON r.data_element_id = c.value       AND r.network = c.network   AND  r.event_status_id  IN(6,11)
    AND r.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
    WHERE
    r.result_value IS NOT NULL
    AND(LOWER(r.result_value) NOT LIKE '%not%'
    AND LOWER(r.result_value) NOT LIKE '%no%record%'
    AND LOWER(r.result_value) NOT LIKE '%n/a%'
    AND LOWER(r.result_value) NOT LIKE '%nn/a%'
    AND LOWER(r.result_value) NOT LIKE '%no%record%'
    AND LOWER(r.result_value) NOT LIKE '%remind%patient%'
    AND LOWER(r.result_value) NOT LIKE '%unable%'
    AND LOWER(r.result_value) NOT LIKE '%none%'
    AND LOWER(r.result_value) NOT LIKE '%na%'
    AND LOWER(r.result_value) NOT LIKE '%not%done%'
    AND LOWER(r.result_value) NOT LIKE '%rt arm%'
    AND LOWER(r.result_value) NOT LIKE '%rt foot%'
    AND LOWER(r.result_value) NOT LIKE '%unable%'
    AND LOWER(r.result_value) NOT LIKE 'pt%agrees%to%work%hard%to%keep%hgb%a1c%below%'
    AND LOWER(r.result_value) NOT LIKE 'determined%in%the%past%'
    AND LOWER(r.result_value) NOT LIKE 'see%note%'
    AND LOWER(r.result_value) NOT LIKE 'not%fasting%'
    AND TRIM(LOWER(r.result_value)) <> 'n')
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
    AND r.network = lkp.network  AND lkp.criterion_id = 13 AND r.network =  SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
   ),
bp_final_tb
AS
(
SELECT --+ materialize 
    network,
    visit_id,
    result_dt AS bp_final_result_dt,
    bp_final_calc_systolic||'/' || bp_final_calc_diastolic as bp_final_calc_value, 
    bp_final_calc_systolic  ,
    bp_final_calc_diastolic,
row_number() over (partition by network,visit_id order by result_dt desc) rnum_per_visit
FROM
  (
    SELECT
   r.network,
   r.visit_id,
   r.event_id,
   r.result_dt,
   TO_CHAR(MAX(bp_calc_systolic)) AS bp_final_calc_systolic,
   TO_CHAR(MAX(bp_calc_diastolic)) AS bp_final_calc_diastolic
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
 v.patient_key,
 v.facility_key,
 v.admission_dt_key,
 v.discharge_dt_key,
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
 p.hypertansion_ind,
 p.kidney_diseases_ind,
 q.criterion_id,
 q.result_dt,
 q.result_value,
 CASE
 WHEN SUBSTR(q.result_value, 1, 1) <> '0' 
     AND REGEXP_COUNT(q.result_value, '\.', 1) <= 1 THEN
      CASE 
       WHEN q.criterion_id IN (10, 23) THEN -- Glucose / LDL
        CASE WHEN   TO_number(SUBSTR(q.result_value, 1, 4)) < 1000 THEN
           REGEXP_SUBSTR(result_value, '^[0-9\.]+')
        END 
       WHEN q.criterion_id = 4 THEN --  A1C
        CASE WHEN SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$'), 1, 38) <=  50 THEN
          SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$'), 1, 5)
         END
       WHEN q.criterion_id = 13 THEN --BP
     REGEXP_SUBSTR(q.result_value, '^[0-9\/]*')
   END
 END
  AS calc_value
  FROM
  fact_visits v 
  LEFT JOIN  v_fact_patient_metric_diag p ON p.patient_id = v.patient_id AND p.network = v.network  
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
  hypertansion_ind,
  kidney_diseases_ind,
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
   IN (4 AS a1c, 23 AS gluc, 10 AS ldl, 13 AS bp))
)
SELECT --+  parallel (32)
 a.network,
 a.visit_id,
 a.patient_key,
 a.facility_key,
 a.admission_dt_key,
 a.discharge_dt_key,
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
 NVL(a.hypertansion_ind, 0) hypertansion_ind,
 NVL(a.kidney_diseases_ind, 0) kidney_diseases_ind,
 a.a1c_final_result_dt,
 a.a1c_final_orig_value,
TO_NUMBER( a.a1c_final_calc_value)a1c_final_calc_value,
 a.gluc_final_result_dt,
 a.gluc_final_orig_value,
TO_NUMBER( a.gluc_final_calc_value) gluc_final_calc_value,
 a.ldl_final_result_dt,
 a.ldl_final_orig_value,
TO_NUMBER( a.ldl_final_calc_value) ldl_final_calc_value,
 NVL(b.bp_final_result_dt, a.bp_final_result_dt) AS bp_final_result_dt,
 NVL(b.bp_final_calc_value, a.bp_final_calc_value) AS bp_final_calc_value,
TO_NUMBER( NVL(b.bp_final_calc_systolic, SUBSTR(a.bp_final_calc_value, 1, INSTR(a.bp_final_calc_value, '/') - 1)))  AS bp_final_calc_systolic,
TO_NUMBER(  NVL(b.bp_final_calc_diastolic, SUBSTR(a.bp_final_calc_value, INSTR(a.bp_final_calc_value, '/') + 1, 3)))   AS bp_final_calc_diastolic
FROM
 final_calc_tb a
 LEFT JOIN bp_final_tb b ON b.network = a.network AND b.visit_id = a.visit_id AND b.rnum_per_visit = 1
WHERE
 NVL(a.asthma_ind, 0) <> 0
 OR NVL(a.bh_ind, 0) <> 0
 OR NVL(a.breast_cancer_ind, 0) <> 0
 OR NVL(a.diabetes_ind, 0) <> 0
 OR NVL(a.heart_failure_ind, 0) <> 0
 OR NVL(a.hypertansion_ind, 0) <> 0
 OR NVL(a.kidney_diseases_ind, 0) <> 0
 OR a.a1c_final_orig_value IS NOT NULL
 OR a.gluc_final_orig_value IS NOT NULL
 OR a.ldl_final_orig_value IS NOT NULL
 OR NVL(b.bp_final_calc_value, a.bp_final_calc_value) IS NOT NULL