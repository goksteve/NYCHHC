ALTER SESSION ENABLE PARALLEL DDL;
ALTER SESSION ENABLE PARALLEL DML;
 
CREATE TABLE steve_del_CBN
NOLOGGING
compress basic
PARALLEL 48 AS
WITH crit_metric AS
      (SELECT --+ materialize
        network, criterion_id, VALUE,
        CASE WHEN criterion_id = 13 THEN
          CASE  
           WHEN UPPER(value_description) LIKE '%SYS%' THEN 'S' -- systolic
           WHEN UPPER(value_description) LIKE '%DIAS%' THEN 'D' -- diastolic
           ELSE 'C' -- combo
          END
        END  AS test_type
       FROM  meta_conditions
       WHERE  criterion_id IN (4,10,23,13)),
     tmp AS
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
       AND NOT REGEXP_LIKE(TRIM(LOWER(r.result_value)), '(^3n)|(^x\[p)|(^kct)|(over)|(a1c)|(^n9)|(other)|(invalid)' )
    )
 SELECT --+ PARALLEL (48)
 q.network,
 q.visit_id,
 q.patient_key,
 q.patient_id,
 q.criterion_id,
 q.result_dt,
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
        TO_NUMBER('0')
 END  AS calc_value
FROM
 tmp q
WHERE
  rnum = 1 and  q.criterion_id < > 13 ;