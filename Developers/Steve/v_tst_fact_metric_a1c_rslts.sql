CREATE OR REPLACE VIEW v_tst_fact_metric_a1c_rslts AS
SELECT
  network,
  visit_id,
  a1c_final_orig_value,
  a1c_final_calc_value
FROM
(
  SELECT
    q.*,
    CASE
      WHEN q.criterion_id = 4 --- RESULTS:DIABETES A1C 
      THEN 
        CASE
          WHEN SUBSTR(q.result_value, 1, 1) <> '0' AND REGEXP_COUNT(q.result_value, '\.', 1) <= 1 AND LENGTH(q.result_value) <= 38 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$')
        END
    END calc_value
  FROM
  (
    WITH 
      crit_junk AS
      (
        SELECT value FROM meta_conditions WHERE criterion_id = 47
      ),
      crit_metric AS
      (
        SELECT  network, criterion_id, value FROM meta_conditions WHERE criterion_id=4 AND include_exclude_ind = 'I'
      ) 
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
     AND r.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
     AND r.result_value IS NOT NULL
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
  ) q
WHERE q.rnum = 1
)
PIVOT
(
  MAX(result_value) AS final_orig_value, MAX(calc_value) AS final_calc_value
  FOR criterion_id IN (4 AS a1c)
);