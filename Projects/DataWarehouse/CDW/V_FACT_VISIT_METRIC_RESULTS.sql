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
    AND (r.result_value NOT LIKE '%not%'
    AND r.result_value NOT LIKE '%no%record%'
    AND r.result_value NOT LIKE '%n/a%'
    AND r.result_value NOT LIKE '%nn/a%'
    AND r.result_value NOT LIKE '%no%record%'
    AND r.result_value NOT LIKE '%remind%patient%'
    AND r.result_value NOT LIKE '%unable%'
    AND r.result_value NOT LIKE '%none%'
    AND r.result_value NOT LIKE '%na%'
    AND r.result_value NOT LIKE '%not%done%'
    AND r.result_value NOT LIKE '%rt arm%'
    AND r.result_value NOT LIKE '%rt foot%'
    AND r.result_value NOT LIKE '%unable%'
    AND r.result_value NOT LIKE 'Pt%agrees%to%work%hard%to%keep%Hgb%A1c%below%'
    AND r.result_value NOT LIKE 'Determined%in%the%past%'
    AND r.result_value NOT LIKE 'See%Note%'
    AND r.result_value NOT LIKE 'Not%Fasting%'
    AND TRIM(r.result_value) <> 'n')
 ),

 bp_rslt AS
   (
    SELECT--+ materialize parallel (32)
    r.network,
    r.visit_id,
    r.event_id,
    r.result_dt,
    CASE
    WHEN lkp.test_type = 'C' THEN REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',1)
    WHEN lkp.test_type = 'S' THEN REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1)
    END AS bp_calc_systolic,      
    CASE                          
    WHEN lkp.test_type = 'C' THEN REGEXP_SUBSTR (r.result_VALUE,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',2)
    WHEN lkp.test_type = 'D' THEN REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1)
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
      SELECT r.network, r.visit_id, r.event_id, r.result_dt,
      MAX (bp_calc_systolic)  as  bp_final_calc_systolic,
      MAX (bp_calc_diastolic)  as bp_final_calc_diastolic
      FROM bp_rslt r
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
 q.criterion_id,
 q.result_dt,
 q.result_value,
 CASE
  WHEN q.criterion_id IN (10, 23) THEN -- Glucose LDL
   REGEXP_SUBSTR(result_value, '^[0-9\.]+')
  WHEN q.criterion_id = 4 THEN --  A1C
   CASE
    WHEN SUBSTR(q.result_value, 1, 1) <> '0'
         AND REGEXP_COUNT(q.result_value, '\.', 1) <= 1
         AND SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$'), 1, 38) <=
              50 THEN
     SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$'), 1, 5)
   END
     WHEN q.criterion_id = 13 THEN --BP
   REGEXP_SUBSTR(q.result_value, '^[0-9\/]*')
 END
  AS calc_value
FROM
         fact_visits v JOIN rslt q ON q.visit_id = v.visit_id AND q.network = v.network AND q.rnum = 1
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
  a.a1c_final_result_dt,
  a.a1c_final_orig_value,
  a.a1c_final_calc_value,
  a.gluc_final_result_dt,
  a.gluc_final_orig_value,
  a.gluc_final_calc_value,
  a.ldl_final_result_dt,
  a.ldl_final_orig_value,
  a.ldl_final_calc_value,
  NVL(b.bp_final_result_dt, a. bp_final_result_dt)   as bp_final_result_dt,
  NVL(b.bp_final_calc_value ,a. bp_final_calc_value) as  bp_final_calc_value,
  NVL(b.bp_final_calc_systolic,SUBSTR(a.bp_final_calc_value, 1, INSTR(a.bp_final_calc_value, '/') - 1)) as   bp_final_calc_systolic,
  NVL(b.bp_final_calc_diastolic, SUBSTR(a.bp_final_calc_value, INSTR(a.bp_final_calc_value, '/') + 1, 3)) AS bp_final_calc_diastolic
  FROM
 final_calc_tb a 
LEFT JOIN bp_final_tb b ON b.network = a.network AND b.visit_id = a.visit_id and b.rnum_per_visit= 1