CREATE OR REPLACE VIEW v_fact_daily_visits_stats_epic
AS
SELECT 
  network,
  facility_key,
  facility_name,
  visit_id,
  admission_dt,
  discharge_dt,
  visit_type,
  patient_key,
  patient_id,
  patient_name,
  mrn,
  birth_date,
  sex,
  age,
  coding_scheme,
  diagnosis_name,
  icd_code,
  is_primary_problem,
  asthma_ind,
  bh_ind,
  breast_cancer_ind,
  diabetes_ind,
  heart_failure_ind,
  hypertension_ind,
  kidney_diseases_ind,
  pregnancy_ind,
  pregnancy_onset_dt,
  nephropathy_screen_ind,
  retinal_eye_exam_ind
FROM 
(
  WITH meta_diag
  AS 
  (
    SELECT --+ materialize
      DISTINCT cnd.VALUE AS VALUE,
      CASE
        WHEN cr.criterion_id IN (1,6,37,50,51,52,58,60,66,68)
        THEN 'diabetes'
        WHEN cr.criterion_id IN (21,48,49,53,57,59)
        THEN 'asthma'
        WHEN cr.criterion_id IN (7,9,31,32)
        THEN 'bh'
        WHEN cr.criterion_id IN (17, 18)
        THEN 'breast_cancer'
        WHEN cr.criterion_id IN (27)
        THEN 'cervical_cancer'
        WHEN cr.criterion_id IN (30,39,70,71)
        THEN 'heart_failure'
        WHEN cr.criterion_id IN (3, 36, 38)
        THEN 'hypertension'
        WHEN cr.criterion_id IN (63, 65)
        THEN 'kidney_diseases'
        WHEN cr.criterion_id IN (73)
        THEN 'pregnancy'
        WHEN cr.criterion_id IN (66)
        THEN 'nephropathy_screen'
        WHEN cr.criterion_id IN (68) 
        THEN 'retinal_dil_eye_exam'
      END AS diag_type_ind,
      cr.criterion_id diag_type_id,
      cr.criterion_cd,
      include_exclude_ind
    FROM pt005.meta_criteria cr
    JOIN pt005.meta_conditions cnd
      ON cnd.criterion_id = cr.criterion_id
    WHERE cr.criterion_id IN (1,3,6,7,9,11,17,18,21,27,30,31,32,36,37,38,39,48,49,50,51,52,53,57,58,59,60,63,65,70,71,73) --and INCLUDE_EXCLUDE_IND  = 'I'
  ),
  diag_pat
  AS 
  (
    SELECT /*+ PARALLEL (32) */
      v.network,
      v.visit_id,
      v.facility_id,
      f.facility_name,
      f.facility_key,
      v.admission_date_time AS admission_dt,
      v.discharge_date_time AS discharge_dt,
      v.visit_type_id,
      (
        CASE
          WHEN visit_type_id = 1
          THEN 'Inpatient'
          WHEN visit_type_id = 2
          THEN 'Emergency'
          WHEN visit_type_id = 3
          THEN 'Outpatient'
          WHEN visit_type_id = 4
          THEN 'Clinic'
          WHEN visit_type_id = 6
          THEN 'Ambulatory Surgery'
          WHEN visit_type_id NOT IN ('1','2','3','4','5','6')
          THEN 'Other Hospital Encounters'
        END
      ) AS visit_type,
      visit_status_id,
      'Z' || v.patient_id AS patient_id,
      99999999 patient_key,
      p.name AS patient_name,
      p.medical_record_number AS mrn,
      p.sex,
      p.birthdate,
      CASE
        WHEN MONTHS_BETWEEN ( ADD_MONTHS (SYSDATE, -1 * 12), p.birthdate)/ 12 > 1
        THEN FLOOR ( MONTHS_BETWEEN (ADD_MONTHS (SYSDATE, -1 * 12),p.birthdate)/ 12)
        ELSE ROUND ((  MONTHS_BETWEEN (ADD_MONTHS (SYSDATE, -1 * 12),p.birthdate)/ 12),1)
      END AS age,
      'ICD-10' AS coding_scheme,
      pdx.dx_id,
      pdx.primary_dx_yn AS is_primary_problem,
      CASE WHEN pdx.primary_dx_yn = 'Y' THEN 1 ELSE 0 END
      AS problem_status_id,
      pdx.comments AS problem_comments,
      edg.current_icd10_list AS icd_code,
      edg.dx_name AS diagnosis_name,
      pdx.line AS problem_nbr,
      TO_NUMBER (TO_CHAR (pdx.contact_date, 'YYYYMMDD')) AS diagnosis_dt_key,
      pdx.contact_date AS onset_date,
      'Y' epic_flag
    FROM ptfinal.s_visit v
    JOIN cdw.dim_hc_facilities f       ON v.network = f.network     AND v.facility_id = f.facility_id
    LEFT JOIN ptfinal.s_patient p      ON v.network = p.network     AND v.patient_id = p.patient_id      AND v.epic_flag = p.epic_flag
    LEFT JOIN epic_clarity.pat_enc_dx pdx      ON v.visit_id = pdx.pat_enc_csn_id   
   LEFT  JOIN epic_clarity.clarity_edg edg       ON pdx.dx_id = edg.dx_id
    WHERE v.epic_flag = 'Y' AND v.admission_date_time >= (SELECT SYSDATE - 10 FROM DUAL) AND v.admission_date_time < (SELECT SYSDATE FROM DUAL)
  ),
  pat_inc_exc AS 
  (
    SELECT 
      diag_type_ind,
      include_exclude_ind,
      d.network,
      d.patient_id,
      d.visit_id
    FROM diag_pat d
    LEFT JOIN meta_diag m ON d.icd_code = m.VALUE
    WHERE m.include_exclude_ind = 'I'
      AND (d.network, d.patient_id) NOT IN
      (
        SELECT 
          d1.network, d1.patient_id
        FROM diag_pat d1
        LEFT JOIN meta_diag m1
          ON d1.icd_code = m1.VALUE
        WHERE m1.include_exclude_ind = 'E'
      )
  )
  SELECT /*+ PARALLEL (32) */
    DISTINCT network,
    facility_key,
    facility_name,
    visit_id,
    admission_dt,
    discharge_dt,
    visit_type,
    patient_key,
    patient_id,
    patient_name,
    mrn,
    birth_date,
    sex,
    age,
    coding_scheme,
    diagnosis_name,
    icd_code,
    is_primary_problem,
    asthma_ind,
    bh_ind,
    breast_cancer_ind,
    diabetes_ind,
    heart_failure_ind,
    hypertension_ind,
    kidney_diseases_ind,
    pregnancy_ind,
    pregnancy_onset_dt,
    nephropathy_screen_ind,
    retinal_eye_exam_ind
  FROM 
  (
    SELECT 
      d.network,
      d.facility_key,
      d.facility_name,
      d.visit_id,
      d.admission_dt,
      d.discharge_dt,
      d.visit_type,
      d.patient_key,
      d.patient_id,
      d.patient_name,
      d.mrn,
      d.birthdate AS birth_date,
      d.sex,
      d.age,
      d.coding_scheme,
      d.onset_date,
      d.diagnosis_dt_key,
      d.icd_code,
      d.diagnosis_name,
      d.is_primary_problem,
      pat_inc.diag_type_ind
    FROM diag_pat d
    LEFT JOIN pat_inc_exc pat_inc
      ON d.network = pat_inc.network
     AND d.patient_id = pat_inc.patient_id
    WHERE 1 = 1 AND d.network IS NOT NULL
  )
  PIVOT
  (
    COUNT (diag_type_ind) AS ind, MAX (onset_date) AS onset_dt FOR diag_type_ind IN 
      (
        'asthma' AS asthma,
        'bh' AS bh,
        'breast_cancer' AS breast_cancer,
        'diabetes' AS diabetes,
        'heart_failure' AS heart_failure,
        'hypertension' AS hypertension,
        'kidney_diseases' AS kidney_diseases,
        'pregnancy' AS pregnancy,
        'nephropathy_screen' AS nephropathy_screen,
        'retinal_dil_eye_exam' AS retinal_eye_exam
      )
  )
);