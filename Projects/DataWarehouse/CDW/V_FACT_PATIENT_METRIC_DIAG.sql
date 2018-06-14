CREATE OR REPLACE VIEW V_FACT_PATIENT_METRIC_DIAG AS
 WITH meta_diag AS
(
 SELECT --+ materialize
           DISTINCT cnd.VALUE AS VALUE,
  CASE
    WHEN cr.criterion_id IN (1,6,37,50,51,52,58,60) THEN  'diabetes'
    WHEN cr.criterion_id IN (21,48,49,53,57,59) THEN      'asthma'
    WHEN cr.criterion_id IN (7,9,31,32) THEN              'bh'
    WHEN cr.criterion_id IN (17,18) THEN                  'breast_cancer'
    WHEN cr.criterion_id IN (27) THEN                     'cervical_cancer'
    WHEN cr.criterion_id IN (30,39,70,71) THEN            'heart_failure'
    WHEN cr.criterion_id IN (3,36,38) THEN                'hypertension'
    WHEN cr.criterion_id IN (63, 65) THEN                 'kidney_diseases'
    WHEN cr.criterion_id IN (73) THEN                     'pregnancy'
    WHEN cr.criterion_id IN(99) then                      'influenza'
    when cr.criterion_id IN(100) then                     'pneumonia'
    when cr.criterion_id IN(11,19,20) then                'bronchitis'
   ELSE
   'N/A'
  END   AS diag_type_ind,
      cr.criterion_id diag_type_id,
      cr.criterion_cd,
     include_exclude_ind
  FROM
   meta_criteria cr JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
  WHERE
   cr.criterion_cd like 'DIAGNOSES%' -- IN (1,3,6,7,9,11,17,18,19,20,21,27,30,31,32,36,37,38,39,48,49,50,51,52,53,57,58,59,60,63,65,70,71,73,99,100) 
  -- and cnd.INCLUDE_EXCLUDE_IND  = 'I'
 ),
pat_smoker
AS
  (
     SELECT
     network,
     patient_id,
    'smoker' AS diag_type_ind,
     NULL onset_date
    FROM
     dim_patients
    WHERE
     current_flag = 1 AND smoker_flag IS NOT NULL
    AND network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
  ),

pat_flu_pna
AS(
      SELECT network,patient_id,onset_date,'influenza' AS diag_type_ind,
      ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY s.onset_date DESC) cnt
      FROM
      fact_patient_diagnoses s 
      WHERE
      problem_status = 'active'    AND s.diag_code = 'Z23'
      AND LOWER(problem_comments) LIKE '%influenza%'  
      AND diag_coding_scheme = 'ICD-10'
      AND s.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
    UNION ALL
      SELECT network,patient_id,onset_date,'pneumoniae' AS diag_type_ind,
      ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY s.onset_date DESC) cnt
      FROM
      fact_patient_diagnoses s 
      WHERE
      problem_status = 'active'    AND s.diag_code = 'Z23'
      AND REGEXP_LIKE (problem_comments, 'pneumoniae|pneumococcus ','i')
      AND diag_coding_scheme = 'ICD-10'
      AND s.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
  )
,
rslt_pat
AS
(
  SELECT --+ materialize
  network, patient_id,diag_type_ind,include_exclude_ind,  onset_date
  FROM
  (
    SELECT
    network,
    patient_id,
    onset_date,
    diag_type_ind,
    include_exclude_ind,
    ROW_NUMBER() OVER(PARTITION BY network, patient_id, diag_type_ind, include_exclude_ind ORDER BY s.onset_date DESC) cnt
    FROM
    fact_patient_diagnoses s JOIN meta_diag d ON s.diag_code = d.VALUE AND d.diag_type_ind <> 'N/A'
    WHERE
    problem_status = 'active'
   AND s.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
  )
  WHERE cnt = 1
)
,
TMP_RES
AS
  (
      SELECT  network, patient_id, diag_type_ind, onset_date
      FROM rslt_pat
      WHERE include_exclude_ind = 'I'
      AND (network, patient_id, diag_type_ind)
          NOT IN 
                ( 
                  SELECT network, patient_id, diag_type_ind 
                  FROM rslt_pat  WHERE include_exclude_ind = 'E'
                )
      UNION
      SELECT network, patient_id, diag_type_ind, onset_date FROM pat_smoker
      UNION
      SELECT network, patient_id, diag_type_ind, onset_date FROM pat_flu_pna where cnt = 1
  ),
tmp_final
AS
  (
   SELECT --+ materialize
   network,
   patient_id,
   asthma_ind,
   bh_ind,
   breast_cancer_ind,
   diabetes_ind,
   heart_failure_ind,
   hypertension_ind,
   kidney_diseases_ind,
   smoker_ind,
   pregnancy_ind,
   pregnancy_onset_dt,
   flu_vaccine_ind,
   flu_vaccine_onset_dt,
   pna_vaccine_ind,
   pna_vaccine_onset_dt,
   bronchitis_ind,
   bronchitis_onset_dt
  FROM
   TMP_RES

  PIVOT
   (
     COUNT(diag_type_ind)  AS ind,
     MAX(onset_date) AS onset_dt
    FOR diag_type_ind  IN 
       (
        'asthma'           AS asthma,
        'bh'               AS bh,
        'breast_cancer'    AS breast_cancer,
        'diabetes'         AS diabetes,
        'heart_failure'    AS heart_failure,
        'hypertension'     AS hypertension,
        'kidney_diseases'  AS kidney_diseases,
        'smoker'           AS smoker,
        'pregnancy'        AS pregnancy,
        'influenza'        AS flu_vaccine ,
        'pneumonia'        AS pna_vaccine ,
        'bronchitis'       AS bronchitis
       )
   )
  )
SELECT
 b.patient_key,
 a.network,
 a.patient_id,
 a.asthma_ind,
 a.bh_ind,
 a.breast_cancer_ind,
 a.diabetes_ind,
 a.heart_failure_ind,
 a.hypertension_ind,
 a.kidney_diseases_ind,
 a.smoker_ind,
 a.pregnancy_ind,
 a.pregnancy_onset_dt,
 a.flu_vaccine_ind,
 a.flu_vaccine_onset_dt,
 a.pna_vaccine_ind,
 a.pna_vaccine_onset_dt,
 a.bronchitis_ind,
 a.bronchitis_onset_dt
FROM
 tmp_final a
 JOIN dim_patients b ON b.network = a.network AND b.patient_id = a.patient_id AND b.current_flag = 1