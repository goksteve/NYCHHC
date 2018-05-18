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
         AS test_type
       FROM
        meta_conditions
       WHERE
        criterion_id IN (4,
                         10,
                         23,
                         13)),
     tmp AS
      (SELECT --+ materialize
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
            AND r.network = 'GP1'
       WHERE
        TRIM(r.result_value) IS NOT NULL
        AND REGEXP_REPLACE(
             TRIM(r.result_value),
             '(([[:digit:]]{1,2})-([[:alpha:]]{2,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{2,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))| ([[:digit:]]{4,10})|([[:alpha:]-\=/.+ ,?!><$*#^@%)(0&])')
             IS NOT NULL
        AND REGEXP_REPLACE(SUBSTR(TRIM(r.result_value), 1, 1), '[-\=/.+ ,?!><$*#^@%)(0&]') IS NOT NULL
        AND SUBSTR(
             TRIM(
              REGEXP_REPLACE(
               REGEXP_REPLACE(
                r.result_value,
                '([-?!.+_]+$)|([[:alpha:]]\.)|([\)(])|(([[:digit:]]{1,2})-([[:alpha:]]{2,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{2,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))'),
               '([[:digit:]]{4})|([^[:digit:]. ])')),
             1,
             1) <> '0'
        AND NOT REGEXP_LIKE(
                 TRIM(LOWER(r.result_value)),
                 '(^kca)|(^xp)|(^precision)|(^will)|(^smc)|(^patient)|(^mrr)|(^m1)|(^mc)|(^mod)| (^mmr)|(pt sent)|(pending)')
        AND (LOWER(r.result_value) NOT LIKE '%not%'
             AND LOWER(r.result_value) NOT LIKE '%n/a%'
             AND LOWER(r.result_value) NOT LIKE '%nn/a%'
             AND LOWER(r.result_value) NOT LIKE '%no%record%'
             AND LOWER(r.result_value) NOT LIKE '%remind%patient%'
             AND LOWER(r.result_value) NOT LIKE '%unable%'
             AND LOWER(r.result_value) NOT LIKE '%none%'
             AND LOWER(r.result_value) NOT LIKE '%na%'
             AND LOWER(r.result_value) NOT LIKE '%not%'
             AND LOWER(r.result_value) NOT LIKE '%rt%arm%'
             AND LOWER(r.result_value) NOT LIKE '%rt%foot%'
             AND LOWER(r.result_value) NOT LIKE '%unable%'
             AND LOWER(r.result_value) NOT LIKE 'pt%agrees%to%work%'
             AND LOWER(r.result_value) NOT LIKE 'determined%in%'
             AND LOWER(r.result_value) NOT LIKE 'see%note%'
             AND LOWER(r.result_value) NOT LIKE '%unknown%'
             AND LOWER(r.result_value) NOT LIKE '%abnormal%'
             AND LOWER(r.result_value) NOT LIKE '%see%scanned%'
             AND LOWER(r.result_value) NOT LIKE '%proteinuria%'))
SELECT --+ PARALLEL (48)
 q.network,
 q.visit_id,
 q.patient_key,
 q.patient_id,
 q.criterion_id,
 q.result_dt,
 q.result_value,
 CASE
  WHEN q.criterion_id IN (10, 23) THEN -- Glucose / LDL
   CASE
    WHEN          SUBSTR(
REGEXP_REPLACE(TRIM( 
REGEXP_REPLACE( 
REGEXP_REPLACE( q.result_value, 
 '(([[:digit:]]{1,2})-([[:alpha:]]{2,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{2,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))| ([[:digit:]]{4,10})'),
       '([-?!.\=/+,?!><$*#^@%)(&]+$)|(^[-?!.0\=/+,?!><$*#@%)(0&]+)|([[:alpha:]-)(#?!$%])')),
'(^[-?!.0\=/+,?!><$*#@%])'),1,3) <= 1000 THEN
    FN_STR_TO_NUMBER(   q.network,  q.result_value,
              SUBSTR(
REGEXP_REPLACE(TRIM( 
REGEXP_REPLACE( 
REGEXP_REPLACE( q.result_value, 
 '(([[:digit:]]{1,2})-([[:alpha:]]{2,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{2,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))| ([[:digit:]]{4,10})'),
       '([-?!.\=/+,?!><$*#^@%)(&]+$)|(^[-?!.0\=/+,?!><$*#@%)(0&]+)|([[:alpha:]-)(#?!$%])')),
'(^[-?!.0\=/+,?!><$*#@%])'),1,3))
   END
  WHEN q.criterion_id = 4 THEN --  A1C
   CASE
    WHEN          SUBSTR(
REGEXP_REPLACE(TRIM( 
REGEXP_REPLACE( 
REGEXP_REPLACE( q.result_value, 
 '(([[:digit:]]{1,2})-([[:alpha:]]{2,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{2,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))| ([[:digit:]]{4,10})'),
       '([-?!.\=/+,?!><$*#^@%)(&]+$)|(^[-?!.0\=/+,?!><$*#@%)(0&]+)|([[:alpha:]-)(#?!$%])')),
'(^[-?!.0\=/+,?!><$*#@%])'),1,4) <= 50 THEN
    
  FN_STR_TO_NUMBER(   q.network,  q.result_value,
         SUBSTR(
REGEXP_REPLACE(TRIM( 
REGEXP_REPLACE( 
REGEXP_REPLACE( q.result_value, 
 '(([[:digit:]]{1,2})-([[:alpha:]]{2,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{2,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))| ([[:digit:]]{4,10})'),
       '([-?!.\=/+,?!><$*#^@%)(&]+$)|(^[-?!.0\=/+,?!><$*#@%)(0&]+)|([[:alpha:]-)(#?!$%])')),
'(^[-?!.0\=/+,?!><$*#@%])'),1,4)
)
   END
  WHEN q.criterion_id = 13 THEN --BP
    TO_NUMBER('0')
 --   REGEXP_SUBSTR(q.result_value, '^[0-9\/]*')

 END
  AS calc_value
FROM
 tmp q
WHERE
 q.criterion_id  AND rnum = 1;


      
       SUBSTR(TRIM(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE( 
       '(([[:digit:]]{1,2})-([[:alpha:]]{2,9})-([[:digit:]]{2,4}))|(([[:digit:]]{1,2}) ([[:alpha:]]{2,9}) ([[:digit:]]{2,4}))|(([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4}))| ([[:digit:]]{4,10})'),
       '([-?!.\=/+,?!><$*#^@%)(&]+$)|(^[-?!.0\=/+,?!><$*#@%)(0&]+)|([[:alpha:]-)(#?!$%])')),
       '(^[-?!.0\=/+,?!><$*#@%])')),1,4)