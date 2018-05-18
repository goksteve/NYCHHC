CREATE OR REPLACE VIEW v_fact_visit_metric_results1 AS
 --create table steve_visit_metric
 --COMPRESS BASIC
 --parallel 32
 --PARTITION BY LIST (network)
 -- SUBPARTITION BY HASH (visit_id)
 --  SUBPARTITIONS 16
 -- (PARTITION cbn VALUES ('CBN'),
 --  PARTITION gp1 VALUES ('GP1'),
 --  PARTITION gp2 VALUES ('GP2'),
 --  PARTITION nbn VALUES ('NBN'),
 --  PARTITION nbx VALUES ('NBX'),
 --  PARTITION qhn VALUES ('QHN'),
 --  PARTITION sbn VALUES ('SBN'),
 --  PARTITION smn VALUES ('SMN'))
 --

 WITH crit_metric AS
       (SELECT
         network, criterion_id, VALUE
        FROM
         meta_conditions
        WHERE
         criterion_id IN (4,
                          10,
                          23,
                          13)), -- A1C, LDL, Glucose,  BP,
      rslt AS
       (SELECT
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
              AND TRIM(r.result_value) <> 'n')),
      calc_result AS
       (SELECT
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
         fact_visits v JOIN rslt q ON q.visit_id = v.visit_id AND q.network = v.network AND q.rnum = 1)
 SELECT
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
  a1c_final_result_date,
  a1c_final_orig_value,
  a1c_final_calc_value,
  gluc_final_result_date,
  gluc_final_orig_value,
  gluc_final_calc_value,
  ldl_final_result_date,
  ldl_final_orig_value,
  ldl_final_calc_value,
  bp_final_result_date,
  bp_final_orig_value,
  SUBSTR(bp_final_calc_value, 1, INSTR(bp_final_calc_value, '/') - 1) AS bp_calc_systolic,
  SUBSTR(bp_final_calc_value, INSTR(bp_final_calc_value, '/') + 1, 3) AS bp_calc_diastolic
 FROM
  calc_result
  PIVOT
   (MAX(result_dt)
   AS final_result_date, MAX(result_value)
   AS final_orig_value, MAX(calc_value)
   AS final_calc_value
   FOR criterion_id
   IN (4 AS a1c, 23 AS gluc, 10 AS ldl, 13 AS bp))