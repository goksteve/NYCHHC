SET TIMING ON;
ALTER SESSION ENABLE PARALLEL DML;

CREATE TABLE tst_gk_fact_vst_metric_rslts
COMPRESS BASIC
NOLOGGING
PARALLEL 32 
AS
WITH 
  crit_metric AS
  (
    SELECT network, criterion_id, value FROM meta_conditions
    WHERE criterion_id IN (4,10,23,13)  --a1c, ldl, glucose, bp
      AND include_exclude_ind = 'I'
  ),
  rslt AS
  (
    SELECT
      r.network,
      r.visit_id,
      r.result_value,
      c.criterion_id,
      ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY r.event_id DESC) rnum
    FROM crit_metric c
    JOIN fact_results r
      ON r.data_element_id = c.VALUE
     AND r.network = c.network
     AND r.result_value IS NOT NULL
--     AND r.network='SBN'	AND r.visit_id=7410913
    WHERE r.result_value NOT LIKE '%not%'
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
  ),
  vst AS
  (
    SELECT 
      r.network,
      r.visit_id,
      dept.service clinic_code, 
      dept.service_type,
      r.result_value,
      r.criterion_id,
      CASE
        -- results: LDL
        WHEN r.criterion_id IN (10, 23) 
        THEN REGEXP_SUBSTR(r.result_value, '^[0-9\.]+') 
        -- results:diabetes a1c
        WHEN r.criterion_id = 4 
        THEN 
          CASE
            WHEN SUBSTR(r.result_value, 1, 1) <> '0' AND REGEXP_COUNT(r.result_value, '\.', 1) <= 1  AND LENGTH(r.result_value) <= 38 --  AND REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$') <= 50
            THEN REGEXP_REPLACE(REGEXP_REPLACE(r.result_value, '[^[:digit:].]'), '\.$')
          END
        -- results: HTN
        WHEN r.criterion_id = 13 
        THEN REGEXP_SUBSTR(r.result_value, '^[0-9\/]*')
      END AS calc_value
    FROM fact_visits v
    LEFT JOIN dim_hc_departments dept
      ON dept.department_key = v.last_department_key
    JOIN rslt r
      ON r.visit_id = v.visit_id AND r.network = v.network AND r.rnum = 1
  )
SELECT
  * 
FROM
(
  SELECT
    vst.network,
    vst.visit_id,
    vst.criterion_id,
    vst.calc_value,
    vst.clinic_code, 
    vst.service_type,
    vst.result_value
  FROM vst   
)
PIVOT
(
  MAX(result_value) AS final_orig_value, MAX(calc_value) AS final_calc_value
  FOR criterion_id IN (4 AS a1c, 23 AS glucose, 10 AS ldl, 13 AS bp)
);
