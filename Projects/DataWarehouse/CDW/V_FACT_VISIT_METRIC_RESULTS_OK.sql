DROP VIEW V_FACT_VISIT_METRIC_RESULTS_OK;

CREATE OR REPLACE FORCE VIEW v_fact_visit_metric_results_ok
(
 network,
 visit_id,
 a1c_final_orig_value,
 a1c_final_calc_value,
 ldl_final_orig_value,
 ldl_final_calc_value,
 glucose_final_orig_value,
 glucose_final_calc_value,
 bp_final_orig_value,
 bp_calc_systolic_bp  ,
 bp_calc_diastolic_bp


) AS
 WITH crit_junk AS
       (SELECT --+ materialize
         VALUE
        FROM
         meta_conditions
        WHERE
         criterion_id = 47),
      crit_metric AS
       (SELECT --+ materialize
         network, criterion_id, VALUE
        FROM
         meta_conditions
        WHERE
         criterion_id IN (4,
                          10,
                          23,
                          13)
         AND include_exclude_ind = 'I') -- A1C, LDL, Glucose,  BP
 SELECT
  network,
  visit_id,
  a1c_final_orig_value,
  a1c_final_calc_value,
  ldl_final_orig_value,
  ldl_final_calc_value,
  glucose_final_orig_value,
  glucose_final_calc_value,
  bp_final_orig_value,
  SUBSTR(bp_final_calc_value, 1, INSTR(bp_final_calc_value, '/') - 1) bp_calc_systolic_bp,
  SUBSTR(bp_final_calc_value, INSTR(bp_final_calc_value, '/') + 1, LENGTH(bp_final_calc_value))  bp_calc_diastolic_bp
 FROM
  (SELECT
    q.*,
    CASE
     WHEN q.criterion_id IN (10, 23) THEN -- result LDL
      REGEXP_SUBSTR(q.result_value, '^[0-9\.]+')
     WHEN q.criterion_id = 4 THEN --- RESULTS:DIABETES A1C
      CASE
       WHEN SUBSTR(REGEXP_SUBSTR(q.result_value, '^[0-9\.]+'), 1, 1) <> '0'
            AND REGEXP_COUNT(REGEXP_SUBSTR(q.result_value, '^[0-9\.]+'), '\.', 1) <= 1
            AND LENGTH(q.result_value) <= 38
            AND REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$') <= 50 THEN
        REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$')
      END
     WHEN q.criterion_id = 13 THEN
      REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\/$')
    END
     calc_value
   FROM
    (SELECT
      r.network,
      r.visit_id,
      r.result_value,
      c.criterion_id,
      ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY r.event_id DESC) rnum
     FROM
      crit_metric c
      JOIN fact_results r
       ON r.data_element_id = c.VALUE
          AND r.network = c.network
          AND r.network = SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')
      JOIN crit_junk t ON r.result_value NOT LIKE t.VALUE) q
   WHERE
    q.rnum = 1)
  PIVOT
   (MAX(result_value) AS final_orig_value, MAX(calc_value) AS final_calc_value
   FOR criterion_id
   IN (4 AS a1c, 10 AS ldl, 23 AS glucose, 13 AS bp));
