CREATE OR REPLACE VIEW V_FACT_DAILY_VISITS_STATS_EPIC
AS
 WITH 
  get_dates
  AS
    (
     SELECT 
     TRUNC(ADD_MONTHS(SYSDATE, - 24), 'MONTH')start_dt from dual
    ) , 
  meta_diag AS
     (

     SELECT --+ MATERIALIZE
      DISTINCT cnd.VALUE AS VALUE,
      CASE WHEN cr.criterion_id IN (1,6,37,50,51,52,58,60,66,68) THEN 'diabetes'
          WHEN cr.criterion_id IN (21,48,49,53,57,59) THEN 'asthma'
          WHEN cr.criterion_id IN (7,9,31,32) THEN 'bh'
          WHEN cr.criterion_id IN (17, 18) THEN 'breast_cancer'
          WHEN cr.criterion_id IN (27) THEN 'cervical_cancer'
          WHEN cr.criterion_id IN (30,39,70,71) THEN 'heart_failure'
          WHEN cr.criterion_id IN (3, 36, 38) THEN 'hypertension'
          WHEN cr.criterion_id IN (63, 65) THEN 'kidney_diseases'
          WHEN cr.criterion_id IN (73) THEN 'pregnancy'
          WHEN cr.criterion_id IN (66) THEN 'nephropathy_screen'
          WHEN cr.criterion_id IN (68) THEN 'retinal_dil_eye_exam'
      END AS diag_type_ind,
        cr.criterion_id diag_type_id,
        cr.criterion_cd,
        include_exclude_ind
    FROM
      meta_criteria cr JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
  WHERE
      cr.criterion_id IN (1,3,6,7,9,11,17,18,21,27,30,31,32,36,37,38,39,48,49,50,51,52,53,57,58,59,60,63,65,70,71,73) --and INCLUDE_EXCLUDE_IND  = 'I'
  ),
   diag_pat AS
    (
      SELECT /*+ MATERIALIZE */
      v.network,
      v.visit_id,
      v.facility_id,
      f.facility_name,
      f.facility_key,
      v.admission_date_time AS admission_dt,
      v.discharge_date_time AS discharge_dt,
      v.visit_type_id,
      CASE      WHEN visit_type_id = 1 THEN  'Inpatient'
        WHEN visit_type_id = 2 THEN         'Emergency'
        WHEN visit_type_id = 3 THEN         'Outpatient'
        WHEN visit_type_id = 4 THEN         'Clinic'
        WHEN visit_type_id = 6 THEN         'Ambulatory Surgery'
        WHEN visit_type_id NOT IN ('1','2','3','4','5','6') THEN     'Other Hospital Encounters'
      END       AS visit_type,
      visit_status_id,
      'Z' || v.patient_id AS patient_id,
      99999999 patient_key,
      p.name AS patient_name,
      p.medical_record_number AS mrn,
      p.sex,
      p.birthdate,
      ROUND(MONTHS_BETWEEN(SYSDATE, p.birthdate) / 12, 1) AS age,
      'ICD-10' AS coding_scheme,
      pdx.dx_id,
      pdx.primary_dx_yn AS is_primary_problem,
      CASE WHEN pdx.primary_dx_yn = 'Y' THEN 1 ELSE 0 END AS problem_status_id,
      pdx.comments AS problem_comments,
      edg.current_icd10_list AS icd_code,
      edg.dx_name AS diagnosis_name,
      pdx.line AS problem_nbr,
    --  TO_NUMBER(TO_CHAR(pdx.contact_date, 'YYYYMMDD')) AS diagnosis_dt_key,
      pdx.contact_date AS onset_date,
      'Y' epic_flag
     FROM
     get_dates
     CROSS JOIN ptfinal.s_visit v
      JOIN cdw.dim_hc_facilities f ON v.network = f.network AND v.facility_id = f.facility_id
      LEFT JOIN ptfinal.s_patient p  ON v.network = p.network AND v.patient_id = p.patient_id AND v.epic_flag = p.epic_flag
      LEFT JOIN epic_clarity.pat_enc_dx pdx ON v.visit_id = pdx.pat_enc_csn_id
      LEFT OUTER JOIN epic_clarity.clarity_edg edg ON pdx.dx_id = edg.dx_id
     WHERE
      v.epic_flag = 'Y' AND v.admission_date_time >= start_dt  -- LAST_DAY(ADD_MONTHS(SYSDATE, -1))
   ),
    pat_inc_exc AS
    (
      SELECT diag_type_ind, include_exclude_ind, d.network,
      d.patient_id, d.visit_id
      FROM  diag_pat d 
      LEFT JOIN meta_diag m ON d.icd_code = m.VALUE
      WHERE
      m.include_exclude_ind = 'I'
      AND (d.network, d.patient_id) NOT IN (
                                            SELECT
                                            d1.network, d1.patient_id
                                            FROM
                                            diag_pat d1 LEFT JOIN meta_diag m1 ON d1.icd_code = m1.VALUE
                                            WHERE
                                            m1.include_exclude_ind = 'E'
                                            )
      ),
ldl AS
(
  SELECT
   /*+ MATERIALIZE */
    l.patient_id,
    l.visit_id,
    l.ldl_order_time,
    l.ldl_result_time,
    l.ldl_calc_value
   FROM
      (
      SELECT
      order_proc.pat_id AS patient_id,
      order_proc.pat_enc_csn_id AS visit_id,
      order_proc.order_time AS ldl_order_time,
      order_proc.result_time AS ldl_result_time,
      REGEXP_REPLACE(REGEXP_REPLACE(res.ord_value, '[^[:digit:].]'), '\.$') ldl_calc_value,
      ROW_NUMBER() OVER( PARTITION BY order_proc.pat_id, order_proc.pat_enc_csn_id ORDER BY order_proc.result_time DESC) AS rn
      FROM
       get_dates
       CROSS JOIN  epic_clarity.order_proc
      LEFT OUTER JOIN epic_clarity.order_results res ON order_proc.order_proc_id = res.order_proc_id
      LEFT JOIN epic_clarity.x_hhc_v_patients_v patientdata  ON patientdata.pat_id = order_proc.pat_id
           AND order_proc.pat_enc_csn_id = patientdata.pat_enc_csn_id
      WHERE
      res.component_id = 766 -- Cholesterol lrr is 766
      AND order_proc.order_status_c = 5
      AND order_proc.result_time >= get_dates.start_dt
      ) l
  WHERE
  l.rn = 1
 ),
bp_final AS
  (
    SELECT /*+ MATERIALIZE */
   pe.pat_enc_csn_id AS visit_id,
   pe.pat_id AS patient_id,
   bp.bp_diastolic,
   bp.bp_systolic,
   bp.meas_value AS BP_orig_value,
   COALESCE(bp.recorded_time, pe.contact_date) AS bp_result_time
  FROM
   get_dates
   CROSS JOIN   epic_clarity.pat_enc pe
   LEFT OUTER JOIN
                  ( 
                  SELECT 
                  CASE
                    WHEN INSTR(measmax1.meas_value, '/') > 0 THEN
                    SUBSTR(measmax1.meas_value, INSTR(measmax1.meas_value, '/') + 1)
                    END
                    AS bp_diastolic,
                    SUBSTR(measmax1.meas_value, 1, INSTR(measmax1.meas_value, '/') - 1 ) AS bp_systolic,
                    measmax1.*
                FROM
              (
               SELECT    measflow.inpatient_data_id,        measflow.flo_meas_id,
                measflow.recorded_time, measflow.meas_value 
                FROM
                   (
                    SELECT 
                    rec.inpatient_data_id,
                    meas.flo_meas_id,
                    meas.recorded_time,
                    meas.meas_value
                    FROM
                    get_dates
                    CROSS JOIN 
                    epic_clarity.ip_flwsht_rec rec INNER JOIN epic_clarity.ip_flwsht_meas meas ON rec.fsd_id = meas.fsd_id
                    WHERE
                    meas.recorded_time >= get_dates.start_dt
                   ) measflow
                WHERE
                  measflow.recorded_time =
                                        (
                                           SELECT MAX(maxflow.recorded_time)
                                           FROM
                                            (
                                              SELECT  rec.inpatient_data_id,
                                              meas.flo_meas_id,  meas.recorded_time,
                                              meas.meas_value    FROM
                                              get_dates
                                              CROSS JOIN    epic_clarity.ip_flwsht_rec rec
                                              INNER JOIN epic_clarity.ip_flwsht_meas meas ON rec.fsd_id = meas.fsd_id
                                              WHERE  meas.recorded_time >= get_dates.start_dt
                                            ) maxflow
                                           WHERE
                                            measflow.inpatient_data_id = maxflow.inpatient_data_id 
                                           AND measflow.flo_meas_id = maxflow.flo_meas_id
                                      )
           ) measmax1
      WHERE
      NOT REGEXP_LIKE(measmax1.meas_value, '[^0-9, /]+')
       ) bp   ON pe.inpatient_data_id = bp.inpatient_data_id AND bp.flo_meas_id = '5'
    WHERE
      pe.contact_date >= get_dates.start_dt
     AND bp.bp_diastolic IS NOT NULL 
     AND bp.bp_systolic IS NOT NULL
   ),
a1c AS
(
 SELECT --+ MATERIALIZE
 order_proc_id,
 patient_id,
 visit_id,
 a1c_value,
 a1c_result_dt
 FROM
(
  SELECT--+ MATERIALIZE
  op.order_proc_id,
  op.pat_id AS patient_id,
  op.pat_enc_csn_id AS visit_id,
  a.ord_num_value AS a1c_value,
  a.result_time AS a1c_result_dt,
  ROW_NUMBER() OVER(PARTITION BY op.pat_id, op.pat_enc_csn_id ORDER BY a.result_time DESC) AS rn1
  FROM
  get_dates
CROSS JOIN   epic_clarity.order_proc op
  INNER JOIN (
                SELECT
                 ore.order_proc_id,
                 ore.component_id,
                 ore.pat_enc_csn_id,
                 ore.ord_value,
                 REGEXP_REPLACE(REGEXP_REPLACE(ore.ord_num_value, '[^[:digit:].]'), '\.$')ord_num_value,
                 ore.result_time,
                 ROW_NUMBER() OVER(PARTITION BY ore.pat_enc_csn_id ORDER BY ore.result_time DESC) rn
                FROM
                 epic_clarity.order_results ore
                WHERE
                 ore.component_id IN (1195, 304155802401)
             ) a  ON op.order_proc_id = a.order_proc_id AND a.rn = 1
              LEFT JOIN epic_clarity.order_status order_status_recent  ON op.order_proc_id = order_status_recent.order_id
      LEFT JOIN (
                  SELECT
                  MAX(order_status_sub.ord_date_real) AS order_id
                  FROM
                  epic_clarity.order_status order_status_sub
                ) os_sub   ON os_sub.order_id = order_status_recent.order_id
    WHERE
      op.result_time >= get_dates.start_dt
    -- FILTER: Contact (Order) Status -  --
    -- 1=Ordered (Sent) ,2=Resulted ,3=Cancelled
    AND order_status_recent.contact_type_c IN (1, 2)
    -- FILTER: Lab Status -  --
    -- 1=In process, 2=Preliminary result, 3=Final result, 4=Edited, 5=Edited Result-FINAL
    AND order_status_recent.lab_status_c IN (3, 5) -- ORD: 115
    -- ** FILTER: Components 'HGB A1C' , 'HEMOGLOBIN A1C POC' ** --
    --AND ORE.COMPONENT_ID IN (1195,304155802401)
    -- FILTER: Only return Child orders, OR if any Lab status is not NULL --
    AND ((CASE WHEN op.future_or_stand IS NULL AND op.instantiated_time IS NOT NULL THEN 1 ELSE 0 END) =
          1
         --OR ORDER_STATUS_Recent.LAB_STATUS_C IS NOT NULL -- ORD:115 (#64719)
         OR (op.lab_status_c IS NOT NULL) --(#64832) -- Note: Same as above if "ORDER_STATUS_Recent.CONTACT_TYPE_C IN (1,2)" 1=Ordered (Sent) ,2=Resulted
                                         ) --and op.pat_id in ('Z1964741','Z4393340','Z4362006')
    ORDER BY 3
 ) a1c_val
WHERE
a1c_val.rn1 = 1
),
tmp_final
AS 
  (
    SELECT --+ MATERIALIZE
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
    retinal_eye_exam_ind,
    ldl_order_time,
    ldl_result_time,
    ldl_calc_value,
    bp_diastolic,
    bp_systolic,
    bp_orig_value,
    bp_result_time,
    a1c_value,
    a1c_result_dt
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
        --  d.diagnosis_dt_key,
          d.icd_code,
          d.diagnosis_name,
          d.is_primary_problem,
          pat_inc.diag_type_ind,
          ldl.ldl_calc_value,
          ldl.ldl_order_time,
          bp1.bp_diastolic,
          bp1.bp_systolic,
          bp1.bp_orig_value,
          bp1.bp_result_time,
          ldl.ldl_result_time,
          a1c.a1c_value,
          a1c.a1c_result_dt
          FROM  diag_pat d
          LEFT JOIN pat_inc_exc pat_inc ON d.network = pat_inc.network AND d.patient_id = pat_inc.patient_id
          LEFT JOIN ldl ldl ON d.patient_id = ldl.patient_id AND d.visit_id = ldl.visit_id
          LEFT JOIN bp_final bp1 ON d.patient_id = bp1.patient_id AND d.visit_id = bp1.visit_id
          LEFT JOIN a1c a1c ON d.patient_id = a1c.patient_id AND d.visit_id = a1c.visit_id
          WHERE  d.network IS NOT NULL
        )
  PIVOT
  (
    COUNT(diag_type_ind) AS ind, 
    MAX(onset_date) AS onset_dt
    FOR diag_type_ind  IN
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
         )

SELECT /*+ PARALLEL (32) */
 DISTINCT 
 f.network,
 f.facility_key,
 f.facility_name,
 f.visit_id,
 TO_NUMBER(TO_CHAR(NVL(admission_dt, DATE '1901-01-01'), 'YYYYMMDD')) AS admission_dt_key,
 admission_dt,
 f.discharge_dt,
 f.visit_type,
 f.patient_key,
 f.patient_id,
 f.patient_name,
 f.mrn,
 f.birth_date AS birthdate,
 f.sex,
 f.age AS patient_age_at_admission,
 f.coding_scheme,
 f.diagnosis_name,
 f.icd_code,
 f.is_primary_problem,
  CASE WHEN asthma_ind > 0 THEN 1 ELSE 0 END AS asthma_ind ,
  CASE WHEN bh_ind > 0 THEN 1 ELSE 0 END AS bh_ind,
  CASE WHEN breast_cancer_ind > 0 THEN 1 ELSE 0 END AS breast_cancer_ind,
  CASE WHEN diabetes_ind > 0 THEN 1  ELSE 0 END AS diabetes_ind,
  CASE WHEN heart_failure_ind > 0 THEN 1 ELSE 0 END AS heart_failure_ind,
  CASE WHEN hypertension_ind > 0 THEN 1  ELSE 0 END AS hypertension_ind,
  CASE WHEN kidney_diseases_ind > 0 THEN 1 ELSE 0 END AS kidney_diseases_ind,
  CASE WHEN sm.SMOKER_IND > 0 then 1 ELSE 0 END AS SMOKER_IND,
  CASE WHEN pregnancy_ind > 0 THEN 1 ELSE 0 END AS pregnancy_ind,
 pregnancy_onset_dt,
  CASE WHEN nephropathy_screen_ind > 0 THEN 1  ELSE 0 END AS  nephropathy_screen_ind,
  CASE WHEN retinal_eye_exam_ind  > 0 THEN 1 ELSE 0 END AS retinal_eye_exam_ind,
 ldl_order_time,
 ldl_result_time,
 ldl_calc_value,
 bp_diastolic,
 bp_systolic,
 bp_orig_value,
 bp_result_time,
 a1c_value,
 a1c_result_dt
FROM
tmp_final f
LEFT JOIN (
           SELECT DISTINCT 1 AS  SMOKER_IND, PAT_ID AS PATIENT_ID
           FROM EPIC_CLARITY.SOCIAL_HX  WHERE SMOKING_TOB_USE_C IN (1,2,3,9,10)
          ) sm ON sm.patient_id  = f.patient_id
WHERE admission_dt < TRUNC(SYSDATE);
