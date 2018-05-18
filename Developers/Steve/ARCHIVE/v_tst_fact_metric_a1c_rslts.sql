SET TIMING ON;
ALTER SESSION ENABLE PARALLEL DML;

CREATE TABLE tst_fact_metric_a1c_rslts
COMPRESS BASIC
NOLOGGING
PARALLEL 32 
AS 
WITH crit_metric
  AS     
  (        
    SELECT 
      network, criterion_id, value
    FROM meta_conditions
    WHERE criterion_id=4 AND include_exclude_ind = 'I'  
  ),   
rslt AS
(		
  SELECT   
    r.network,    
    r.visit_id, 
    r.result_dt,
    r.result_value,   
    ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id ORDER BY r.event_id DESC) rnum  
  FROM crit_metric c     
  JOIN fact_results r  
    ON r.data_element_id = c.VALUE     
   AND r.network = c.network    
   AND r.result_value IS NOT NULL    
  WHERE 
  ( 
    r.result_value NOT LIKE '%not%'     
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
  )
  AND r.result_dt >= ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) AND r.result_dt < TRUNC(SYSDATE, 'MONTH')
)
SELECT 
  q.network,
  q.visit_id, 
  q.result_value AS a1c_final_orig_value,
  CASE
  WHEN SUBSTR(q.result_value, 1, 1) <> '0'
  AND REGEXP_COUNT(q.result_value, '\.', 1) <= 1
  AND LENGTH(q.result_value) <= 38 
  THEN
  REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$')
  END calc_value
FROM rslt q WHERE q.rnum=1;   