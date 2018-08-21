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
    WHEN cr.criterion_id IN( 31) THEN                     'schizophrenia'
    WHEN cr.criterion_id IN( 32) THEN                     'bipolar'
    WHEN cr.criterion_id IN (3,36,38) THEN                'hypertension'
    WHEN cr.criterion_id IN (63, 65) THEN                 'kidney_diseases'
    WHEN cr.criterion_id IN (73) THEN                     'pregnancy'
    WHEN cr.criterion_id IN(99) THEN                      'influenza'
    WHEN cr.criterion_id IN(100) THEN                     'pneumonia'
    WHEN cr.criterion_id IN(11,19,20) THEN                'bronchitis'
    WHEN cr.criterion_id IN(102) THEN                     'tabacco_screen_diag'
    WHEN cr.criterion_id IN( 104) THEN                    'major_depression'
   ELSE
   'N/A'
  END   AS diag_type_ind 
  --  include_exclude_ind
  FROM
   meta_criteria cr JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
  WHERE
   cr.criterion_cd like 'DIAGNOSES%' -- IN (1,3,6,7,9,11,17,18,19,20,21,27,30,31,32,36,37,38,39,48,49,50,51,52,53,57,58,59,60,63,65,70,71,73,99,100) 
  AND cnd.INCLUDE_EXCLUDE_IND  = 'I'
 ),
pat_smoker
AS
  (
     SELECT
     network,
     patient_id,
    'smoker' AS diag_type_ind,
     null f_onset_dt,
     NULL l_onset_dt
    FROM
     dim_patients
    WHERE
     current_flag = 1 AND smoker_flag IS NOT NULL
 --   AND network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
  ),

pat_flu_pna
AS(
      SELECT network,patient_id,onset_date,'influenza' AS diag_type_ind,
      MIN( onset_date ) over (PARTITION BY network, patient_id ) as f_onset_dt,
      ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY s.onset_date DESC) cnt
      FROM
      fact_patient_diagnoses s 
      WHERE
      problem_status = 'active'    AND s.diag_code = 'Z23'
      AND LOWER(problem_comments) LIKE '%influenza%'  
       --   AND s.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
    UNION ALL
      SELECT network,patient_id,onset_date,'pneumoniae' AS diag_type_ind,
      MIN( onset_date ) over (PARTITION BY network, patient_id ) as f_onset_dt,
      ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY s.onset_date DESC) cnt
      FROM
      fact_patient_diagnoses s 
      WHERE
      problem_status = 'active'    AND s.diag_code = 'Z23'
      AND REGEXP_LIKE (problem_comments, 'pneumoniae|pneumococcus ','i')
        --  AND s.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
  )
,
rslt_pat
AS
(
  SELECT --+ materialize
  network, patient_id,diag_type_ind, f_onset_dt, l_onset_dt, l_end_dt
  FROM
  (
    SELECT
    network,
    patient_id,
    onset_date as  l_onset_dt,
    diag_type_ind,
    MIN(s.onset_date) over ( PARTITION BY network, patient_id, diag_type_ind ) f_onset_dt,
    MAX(s.end_date) over ( PARTITION BY network, patient_id, diag_type_ind)   l_end_dt,
    ROW_NUMBER() OVER(PARTITION BY network, patient_id, diag_type_ind ORDER BY s.onset_date DESC) cnt
    FROM
    fact_patient_diagnoses s JOIN meta_diag d ON s.diag_code = d.VALUE AND d.diag_type_ind <> 'N/A'
    WHERE
    problem_status = 'active'
--   AND s.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
  )
  WHERE cnt = 1
)
,
TMP_RES
AS
  (
      SELECT  network, patient_id, diag_type_ind, f_onset_dt , l_onset_dt, l_end_dt
      FROM rslt_pat
--      WHERE include_exclude_ind = 'I'
--      AND (network, patient_id, diag_type_ind)
--          NOT IN 
--                ( 
--                  SELECT network, patient_id, diag_type_ind 
--                  FROM rslt_pat  WHERE include_exclude_ind = 'E'
--                )
      UNION
      SELECT network, patient_id, diag_type_ind, f_onset_dt,  l_onset_dt, null as l_end_dt FROM pat_smoker
      UNION
      SELECT network, patient_id, diag_type_ind, f_onset_dt, onset_date as l_onset_dt, null as l_end_dt FROM pat_flu_pna where cnt = 1
  ),
tmp_final
AS
  (
SELECT --+ materialize
 network,
 patient_id,
 diabetes_ind,
 diabetes_f_onset_dt,
 diabetes_l_onset_dt,
 diabetes_l_end_dt,
 asthma_ind,
 asthma_f_onset_dt,
 asthma_l_onset_dt,
 asthma_l_end_dt,
 bh_ind,
 bh_f_onset_dt,
 bh_l_onset_dt,
 bh_l_end_dt,
 breast_cancer_ind,
 breast_cancer_f_onset_dt,
 breast_cancer_l_onset_dt,
 breast_cancer_l_end_dt,
 cervical_cancer_ind,
 cervical_cancer_f_onset_dt,
 cervical_cancer_l_onset_dt,
 cervical_cancer_l_end_dt,
 heart_failure_ind,
 heart_failure_f_onset_dt,
 heart_failure_l_onset_dt,
 heart_failure_l_end_dt,
 schizophrenia_ind,
 schizophrenia_f_onset_dt,
 schizophrenia_l_onset_dt,
 schizophrenia_l_end_dt,
 bipolar_ind,
 bipolar_f_onset_dt,
 bipolar_l_onset_dt,
 bipolar_l_end_dt,
 htn_ind,
 htn_f_onset_dt,
 htn_l_onset_dt,
 htn_l_end_dt,
 kidney_dz_ind,
 kidney_dz_f_onset_dt,
 kidney_dz_l_onset_dt,
 kidney_dz_l_end_dt,
 smoker_ind,
 smoker_f_onset_dt,
 smoker_l_onset_dt,
 smoker_l_end_dt,
 pregnancy_ind,
 pregnancy_f_onset_dt,
 pregnancy_l_onset_dt,
 pregnancy_l_end_dt,
 flu_vaccine_ind,
 flu_vaccine_f_onset_dt,
 flu_vaccine_l_onset_dt,
 flu_vaccine_l_end_dt,
 pna_vaccine_ind,
 pna_vaccine_f_onset_dt,
 pna_vaccine_l_onset_dt,
 pna_vaccine_l_end_dt,
 bronchitis_ind,
 bronchitis_f_onset_dt,
 bronchitis_l_onset_dt,
 bronchitis_l_end_dt,
 tabacco_diag_ind,
 tabacco_diag_f_onset_dt,
 tabacco_diag_l_onset_dt,
 tabacco_diag_l_end_dt,
 major_depression_ind,
 major_depression_f_onset_dt,
 major_depression_l_onset_dt,
 major_depression_l_end_dt
FROM
TMP_RES

PIVOT
  (
   COUNT(diag_type_ind)  AS ind,
   MIN (f_onset_dt) AS f_onset_dt,
   MAX(l_onset_dt)  AS  l_onset_dt,
   MAX( l_end_dt) AS l_end_dt
  FOR diag_type_ind  IN 
     (
      'diabetes'  AS diabetes,
      'asthma' AS asthma,
      'bh'  AS bh,
      'breast_cancer' AS breast_cancer,
      'cervical_cancer' AS cervical_cancer,
      'heart_failure'  AS heart_failure,
      'schizophrenia' as schizophrenia,
      'bipolar'   as bipolar,
      'hypertension' AS htn,
      'kidney_diseases'  AS kidney_dz,
      'smoker'                AS smoker,
      'pregnancy'   AS pregnancy,
      'influenza'   AS flu_vaccine ,
      'pneumonia'    AS pna_vaccine ,
      'bronchitis'   AS bronchitis,
      'tabacco_screen_diag' AS tabacco_diag ,
      'major_depression' as major_depression
     )
  )
)
SELECT
 b.patient_key,
 a.network,
 a.patient_id,
 a.diabetes_ind,
 a.diabetes_f_onset_dt,
 a.diabetes_l_onset_dt,
 a.diabetes_l_end_dt,
 a.asthma_ind,
 a.asthma_f_onset_dt,
 a.asthma_l_onset_dt,
 a.asthma_l_end_dt,
 a.bh_ind,
 a.bh_f_onset_dt,
 a.bh_l_onset_dt,
 a.bh_l_end_dt,
 a.breast_cancer_ind,
 a.breast_cancer_f_onset_dt,
 a.breast_cancer_l_onset_dt,
 a.breast_cancer_l_end_dt,
 a.cervical_cancer_ind,
 a.cervical_cancer_f_onset_dt,
 a.cervical_cancer_l_onset_dt,
 a.cervical_cancer_l_end_dt,
 a.heart_failure_ind,
 a.heart_failure_f_onset_dt,
 a.heart_failure_l_onset_dt,
 a.heart_failure_l_end_dt,
 a.schizophrenia_ind,
 a.schizophrenia_f_onset_dt,
 a.schizophrenia_l_onset_dt,
 a.schizophrenia_l_end_dt,
 a.bipolar_ind,
 a.bipolar_f_onset_dt,
 a.bipolar_l_onset_dt,
 a.bipolar_l_end_dt,
 a.htn_ind,
 a.htn_f_onset_dt,
 a.htn_l_onset_dt,
 a.htn_l_end_dt,
 a.kidney_dz_ind,
 a.kidney_dz_f_onset_dt,
 a.kidney_dz_l_onset_dt,
 a.kidney_dz_l_end_dt,
 a.smoker_ind,
 a.smoker_f_onset_dt,
 a.smoker_l_onset_dt,
 a.smoker_l_end_dt,
 a.pregnancy_ind,
 a.pregnancy_f_onset_dt,
 a.pregnancy_l_onset_dt,
 a.pregnancy_l_end_dt,
 a.flu_vaccine_ind,
 a.flu_vaccine_f_onset_dt,
 a.flu_vaccine_l_onset_dt,
 a.flu_vaccine_l_end_dt,
 a.pna_vaccine_ind,
 a.pna_vaccine_f_onset_dt,
 a.pna_vaccine_l_onset_dt,
 a.pna_vaccine_l_end_dt,
 a.bronchitis_ind,
 a.bronchitis_f_onset_dt,
 a.bronchitis_l_onset_dt,
 a.bronchitis_l_end_dt,
 a.tabacco_diag_ind,
 a.tabacco_diag_f_onset_dt,
 a.tabacco_diag_l_onset_dt,
 a.tabacco_diag_l_end_dt,
 a.major_depression_ind,
 a.major_depression_f_onset_dt,
 a.major_depression_l_onset_dt,
 a.major_depression_l_end_dt
FROM
 tmp_final a
 JOIN dim_patients b ON b.network = a.network AND b.patient_id = a.patient_id AND b.current_flag = 1