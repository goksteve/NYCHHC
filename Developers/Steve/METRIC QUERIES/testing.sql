alter session enable parallel DDL;

create table steve_del
nologging
parallel 32
as

WITH crit_metric AS
      (SELECT --+ materialize
        network,
        criterion_id,
        VALUE,
        CASE
         WHEN criterion_id = 13 THEN
          CASE
           WHEN UPPER(value_description) LIKE '%SYS%' THEN 'S' -- systolic
           WHEN UPPER(value_description) LIKE '%DIAS%' THEN 'D' -- diastolic
           ELSE 'C' -- combo
          END
        END
         test_type
       FROM
        meta_conditions
       WHERE
        criterion_id IN (4,
                         10,
                         23,
                         13)),
tmp
AS(
SELECT --+ parallel(32) 
 r.network,
 r.visit_id,
 r.patient_key,
 r.patient_id,
 result_dt,
 TRIM(r.result_value) AS result_value,
 c.criterion_id,
 ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY result_dt DESC) rnum
FROM
 crit_metric c
 JOIN fact_results r
  ON r.data_element_id = c.VALUE
     AND r.network = c.network
     AND r.event_status_id IN (6, 11)
     AND r.network = 'CBN'
WHERE
  criterion_id  <> 13
 AND TRIM(r.result_value) IS NOT NULL
 AND 
   ( REGEXP_replace (TRIM(r.result_value),'[[:alpha:]\/.+-,''?>]', '-1') <> '-1'
     AND  REGEXP_replace (TRIM(r.result_value),'[[:alpha:]\/.+-,''?>]', '-1') <> '-1-1'
     AND REGEXP_replace (TRIM(r.result_value), '[[:alpha:]\/.+-,''?>]', '-1') <> '-1-1-1'
     AND REGEXP_replace (TRIM(r.result_value), '[[:alpha:]\/.+-,''?>]', '-1') <> '-1-1-1-1'
     AND REGEXP_replace (TRIM(r.result_value), '[[:alpha:]\/.+-,''?>]', '-1') <> '-1-1-1-1-1'
     AND REGEXP_replace (TRIM(r.result_value), '[[:alpha:]\/.+-,''?>]') <> '0'
   --AND SUBSTR(TRIM(r.result_value), 1, 1) <> '0'
    AND SUBSTR(TRIM(r.result_value), 1, 1) <> '.'
    AND REGEXP_COUNT(r.result_value, '\.', 1) <= 1
    )
 AND (LOWER(r.result_value) NOT LIKE '%not%'
      AND LOWER(r.result_value) NOT LIKE '%no%record%'
      AND LOWER(r.result_value) NOT LIKE '%n/a%'
      AND LOWER(r.result_value) NOT LIKE '%nn/a%'
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
      AND LOWER(r.result_value) NOT LIKE '%unknown%'
     AND LOWER(r.result_value) NOT LIKE 'abnormal%high%')
    --      AND   TRIM(LOWER(r.result_value)) <> 'nn')
    --      AND TRIM(LOWER(r.result_value)) <> 'no'
    --  AND TRIM(LOWER(r.result_value)) <> 'n'
    -- AND  TRIM(r.result_value) <> '\'
    --  AND   TRIM(LOWER(r.result_value)) <> 'u'
    --  AND   TRIM(LOWER(r.result_value)) <> 'Y'
)

SELECT --+ PARALLEL (32)
 q.network,
 q.visit_id,
 q.patient_key,
 q.patient_id,
 q.criterion_id,
 q.result_dt,
 q.result_value,
 CASE
     WHEN q.criterion_id IN (10, 23) THEN -- Glucose / LDL
       CASE WHEN SUBSTR(REGEXP_REPLACE (REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[\)(]'),'([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})'), '[^[:digit:].]'), '\.$'), 1, 5) < =  1000 THEN
             TO_NUMBER(   SUBSTR ( REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE (q.result_value,'[\)(]'),'([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})'), '[^[:digit:].]'), 1, 5))
        END 
       WHEN q.criterion_id = 4 THEN --  A1C
        CASE WHEN SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[\)(]'),'([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})'), '[^[:digit:].]'), '\.$'), 1, 5) <=  50 THEN
          TO_NUMBER(  SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[\)(]'),'([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})'), '[^[:digit:].]'), '\.$'), 1, 5))
         END
       WHEN q.criterion_id = 13 THEN --BP
    to_number(  REGEXP_SUBSTR(q.result_value, '^[0-9\/]*'))
     
 END AS calc_value
   FROM

  tmp q
WHERE  rnum = 1
 

                  REGEXP_REPLACE(REGEXP_REPLACE(TRIM(r.result_value), '[-\=/.+,?><$*_ ]'), '[[:alpha:]]', '-1') <> '-1'
              AND REGEXP_REPLACE(REGEXP_REPLACE(TRIM(r.result_value), '[-\=/.+,?><$*_ ]'),'[[:alpha:]]', '-1') <> '-1-1'
              AND REGEXP_REPLACE(REGEXP_REPLACE(TRIM(r.result_value), '[-\=/.+,?><$*_ ]'), '[[:alpha:]]', '-1') <> '-1-1-1'
              AND REGEXP_REPLACE( REGEXP_REPLACE(TRIM(r.result_value), '[-\=/.+,?><$*_ ]'), '[[:alpha:]]', '-1') <> '-1-1-1-1'
              AND REGEXP_REPLACE(REGEXP_REPLACE(TRIM(r.result_value), '[-\=/.+,?><$*_ ]'),'[[:alpha:]]', '-1') <> '-1-1-1-1-1'
              AND REGEXP_REPLACE(REGEXP_REPLACE(TRIM(r.result_value), '[-\=/.+,?><$*_ ]'),'[[:alpha:]]',  '-1') <> '-1-1-1-1-1-1'

   -- AND REGEXP_REPLACE(TRIM(r.result_value), '[[:alpha:]\/.+-,''?>]') <> '0'
             -- AND SUBSTR(TRIM(r.result_value), 1, 1) <> '.'
 ---AND REGEXP_COUNT(r.result_value, '\.', 1) <= 1

 ---AND REGEXP_COUNT(r.result_value, '\.', 1) <= 1

--AND  REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(r.result_value), '[-\=/.+,?!><$*_ @])('), '[[:alpha:]]', '-1'),'-1') is NOT null
 --  AND REGEXP_REPLACE(TRIM(r.result_value), '[[:alpha:]\=/.+-,''?>)( 0]') IS NOT NULL