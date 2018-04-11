WITH 
  dt AS 
  (
    SELECT --+ materialize
      TRUNC(SYSDATE, 'MONTH') AS report_period_start_dt, TRUNC(SYSDATE, 'YEAR') AS msrmnt_yr_start_dt, DATE '2018-01-01' begin_dt, DATE '2018-02-01' end_dt
    FROM DUAL
  ),
  htn_ptnt_lkp AS 
  (
    SELECT --+ materialize
      p.network, p.patient_id, p.onset_date, p.diag_code htn_dx_code,
      ROW_NUMBER() OVER (PARTITION BY p.patient_id ORDER BY p.onset_date DESC, p.diag_coding_scheme ASC) htn_ptnt_rnum   
    FROM dt
    JOIN fact_patient_diagnoses p 
      ON p.onset_date >= msrmnt_yr_start_dt AND p.network='CBN'
     AND p.onset_date <   
         CASE 
            WHEN TO_CHAR(dt.end_dt,'mm/dd') < '06/01'
            THEN TRUNC(dt.end_dt,'MONTH')
            ELSE TO_DATE('07/01','MM/DD')
         END 
     AND p.status_id IN (0, 6, 7, 8)
    JOIN meta_conditions mc
      ON mc.value=p.diag_code AND mc.criterion_id=36 AND include_exclude_ind='I' 
    WHERE NOT EXISTS
      (
        SELECT 
          distinct p1.patient_id
        FROM dt
        JOIN fact_patient_diagnoses p1 ON p1.onset_date >= msrmnt_yr_start_dt 
         AND p1.onset_date <   
             CASE 
              WHEN TO_CHAR(dt.end_dt,'mm/dd') < '06/01'
              THEN TRUNC(dt.end_dt,'MONTH')
              ELSE TO_DATE('07/01','MM/DD')
             END  
         AND p1.status_id IN (0, 6, 7, 8) AND p1.network='CBN'
        JOIN meta_conditions mc1
          ON mc1.value=p1.diag_code AND mc1.criterion_id=36 AND mc1.include_exclude_ind='E'  
        WHERE p1.patient_id=p.patient_id     
      )
    ), --321 htn patients
  diab_prob_pat AS
  (    
    SELECT 
      g.patient_id, g.network, MAX(g.onset_date) onset_date, MAX(g.diag_code) diag_code
    FROM  
    (
      SELECT 
        cmv.patient_id, cmv.network, cmv.onset_date, cmv.diag_code
        FROM cdw.fact_patient_diagnoses cmv
        JOIN meta_conditions mc
        ON mc.value = cmv.diag_code AND mc.criterion_id = 37 AND mc.include_exclude_ind = 'I'
        WHERE
        NOT EXISTS
        (
          SELECT 
            DISTINCT cmv1.patient_id,cmv1.network
          FROM cdw.fact_patient_diagnoses cmv1
          JOIN meta_conditions mc1 ON mc1.value=cmv1.diag_code AND mc1.criterion_id = 37 AND mc1.include_exclude_ind = 'E'
          WHERE cmv1.patient_id=cmv.patient_id and cmv1.network=cmv.network   
        )  
      UNION
      SELECT 
        DISTINCT a.patient_id, network, null onset_date, null diag_code
      FROM pt005.fact_prescriptions a
      JOIN ref_drug_descriptions b ON a.drug_description = b.drug_description AND b.drug_type_id = 33
    )g
    GROUP BY g.network,g.patient_id 
  ), 
   htn_op_visits AS 
  (
    SELECT --+ materialize 
      v.network, v.src_patient_id, v.src_visit_id, v.final_visit_type_id, v.last_department_key, v.bp_calc_systolic, v.bp_calc_diastolic, v.facility_key,v.primary_payer_key,
      v.admission_dt, v.discharge_dt, decode(dp.patient_id,NULL,'N','Y') diabetic_flag,
      dp.diag_code diabetes_dx_code, dp.onset_date diabetes_onset_date, 
      lkp.onset_date, lkp.htn_dx_code, htn_flag,
      row_number() over (partition by v.network,v.src_patient_id order by nvl2(bp_calc_systolic,0,1) nulls last, admission_dt desc)  ltst_bp_reading_rnum
    FROM dt
    JOIN tst_uk_f_cl_vst_diag_cbn v
      ON v.admission_dt >= dt.begin_dt AND v.admission_dt < dt.end_dt AND v.network = 'CBN' AND v.final_visit_type_id IN (3,4) 
    JOIN htn_ptnt_lkp lkp
      ON lkp.patient_id=v.src_patient_id AND lkp.network = 'CBN' AND lkp.htn_ptnt_rnum=1  
    LEFT JOIN diab_prob_pat dp
      ON dp.network = v.network AND dp.patient_id = v.src_patient_id AND dp.network = 'CBN'
  )
  SELECT --+ parallel(32) 
  a.network, 
  f.facility_name,         
  a.src_patient_id, 
  p.name patient_name, 
  p.medical_record_number,
  p.birthdate, 
  p.apt_suite,		
  p.street_address,	
  p.city,			
  p.state,			
  p.country,			
  p.mailing_code,	
  p.home_phone,
  FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) AS age,         
  vt.name visit_type_name,       
  a.src_visit_id,
  dept.specialty_code clinic_code,
  dept.service clinic_code_service,	
  dept.specialty clinic_code_desc,    
  a.admission_dt,
  a.discharge_dt,    
  dp.payer_group, 
  dp.payer_name, 
  CASE 
		WHEN FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - p.birthdate)/365) BETWEEN 18 AND 59 
		THEN 'Y' 
		ELSE 'N' 
	END AGE_18_59,
	CASE 
		WHEN FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - p.birthdate)/365) BETWEEN 60 AND 85 
		THEN 'Y' 
		ELSE 'N' 
	END AGE_60_85,
  a.diabetic_flag diabetic,  
  a.diabetes_dx_code,
  a.diabetes_onset_date,
  a.htn_dx_code hypertension_dx_code,
  a.onset_date hypertension_onset_date,   
--  bp_reading_time,       
  a.bp_calc_systolic systolic_bp,           
  a.bp_calc_diastolic diastolic_bp,          
  CASE 
      WHEN ((FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 18 AND 59)AND (bp_calc_systolic < 140 AND bp_calc_diastolic <90))
      THEN 'Y'
      ELSE 'N'
    END numerator_flag1,
    CASE 
      WHEN FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 60 AND 85 AND (bp_calc_systolic < 140 AND bp_calc_diastolic <90) AND diabetic_flag = 'Y'
      THEN 'Y'
      ELSE 'N'
    END numerator_flag2,
    CASE 
      WHEN FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 60 AND 85 AND (bp_calc_systolic < 150 AND bp_calc_diastolic <90) AND diabetic_flag = 'N'
      THEN 'Y'
      ELSE 'N'
    END numerator_flag3
  FROM htn_op_visits a
  JOIN DIM_HC_FACILITIES f
    ON f.facility_key = a.facility_key
  JOIN DIM_PATIENTS P
    ON p.patient_id = a.src_patient_id  AND p.current_flag=1 AND p.network = 'CBN'
  JOIN ref_visit_types vt
    ON vt.visit_type_id = a.final_visit_type_id 
  LEFT JOIN dim_hc_departments dept
    ON a.last_department_key = dept.department_key
  LEFT JOIN dim_payers dp
    ON dp.payer_key=a.primary_payer_key 
  WHERE ltst_bp_reading_rnum=1;