-- SELECT --+ materialize
--           d.network,
--           d.patient_id,
--           d.onset_date,
--           d.diag_code icd_code,
--           d.problem_comments,
--           mc.include_exclude_ind,
--           ROW_NUMBER()    OVER(PARTITION BY d.network, d.patient_id, include_exclude_ind ORDER BY d.onset_date DESC)
--            rnum
--          FROM
--           meta_conditions mc
--           JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE
--          WHERE
--           mc.criterion_id = 1
--           AND d.status_id IN (0,6,7,8)

ALTER SESSION ENABLE PARALLEL DDL;
ALTER SESSION ENABLE PARALLEL DML;
DROP TABLE steve_del_pat_list_24m_a1c;

CREATE TABLE steve_del_pat_list_24m_a1c
NOLOGGING
COMPRESS BASIC
PARALLEL 32 AS
 SELECT /*+ PARALLEL (32) */
  *
 FROM
  (
   WITH val_list_Y AS
         (SELECT
           VALUE
          FROM
           meta_conditions mc
          WHERE
           mc.criterion_id = 1 AND mc.condition_type_cd = 'DI' AND include_exclude_ind = 'I'),

val_list_N AS
         (SELECT
           VALUE
          FROM
           meta_conditions mc
          WHERE
           mc.criterion_id = 1 AND mc.condition_type_cd = 'DI' AND include_exclude_ind = 'E'),



        pat_list_all AS
         (SELECT
           p.*,
           d.onset_date,
           d.diag_code icd_code,
           d.problem_comments,
           ROW_NUMBER() OVER(PARTITION BY d.network, d.patient_id ORDER BY d.onset_date DESC)  rnum
          FROM
           val_list_Y m
           JOIN fact_patient_diagnoses d ON d.diag_code = m.VALUE  AND d.status_id IN (0,6,7,8)
           JOIN steve_dele_pat_list_24m p ON p.patient_id = d.patient_id AND p.network = d.network
          WHERE   ( d.patient_id) NOT IN (
                                  SELECT
                                    patient_id
                                  FROM
                                   val_list_N m JOIN fact_patient_diagnoses d ON d.diag_code = m.VALUE ) )
                   
   SELECT
    *
   FROM
    pat_list_all
   WHERE rnum = 1
)
 
    
                                  
                                 
  