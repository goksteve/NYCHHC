create table steve_del_nephr_eye
nologging
compress basic
parallel 32
AS
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
    WHERE criterion_id IN (4,10,23,13,66,68)
   )-- A1C, LDL, Glucose,  BP, 68-eye exam, Nephropathy - 66

  SELECT --+ parallel (32)  
    r.network,  r.visit_id, r.patient_key, r.patient_id, result_dt,
    TRIM(r.result_value) AS result_value, c.criterion_id,
    ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY result_dt DESC) rnum
  FROM
    crit_metric c
    JOIN fact_results r   ON r.data_element_id = c.VALUE  AND r.network = c.network AND r.event_status_id IN (6, 11)
    AND r.network = 'GP2'
  WHERE
    c.criterion_id in (66,68)
    AND TRIM(r.result_value) IS NOT NULL;

