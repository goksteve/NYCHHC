WITH 
  pat_bp_results AS
  (
    SELECT 
      r.network,
      r.patient_id,
      NVL(psn.medical_record_number, p.medical_record_number) AS mrn,
      p.name patient_name,
      TRUNC(p.birthdate) AS dob,
      TRUNC(p.date_of_death) AS date_of_death,
      apt_suite,		
      street_address,	
      city,			
      state,			
      country,			
      mailing_code,	
      home_phone,
      day_phone,
      FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) AS age, 
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
      f.facility_name AS facility_name, 
      r.visit_type_name,
      r.visit_id,
      r.clinic_code,
      r.clinic_code_service,
      r.clinic_code_desc,
      r.admission_date_time,
      r.discharge_date_time,
      r.onset_date,
      r.htn_dx_code,
      r.event_id,
      r.date_time bp_reading_time,
      r.systolic_bp,
      r.diastolic_bp,
      r.financial_class_id,
      r.visit_financial_class,
      r.payer_id,
      pm.payer_name,
      pm.payer_group,
      CASE
        WHEN diab_prob_pat.patient_id IS NOT NULL
        THEN 'Y'
        ELSE 'N'
      END diabetic, 
      diab_prob_pat.diag_code diabetes_dx_code,
      CASE 
        WHEN ((FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 18 AND 59)AND (systolic_bp < 140 AND diastolic_bp <90))
        THEN 'Y'
        ELSE 'N'
      END numerator_flag1,
      CASE 
        WHEN FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 60 AND 85 AND (systolic_bp < 140 AND diastolic_bp <90) AND diab_prob_pat.patient_id IS NOT NULL
        THEN 'Y'
        ELSE 'N'
      END numerator_flag2,
      CASE 
        WHEN FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 60 AND 85 AND (systolic_bp < 150 AND diastolic_bp <90) AND diab_prob_pat.patient_id IS NULL
        THEN 'Y'
        ELSE 'N'
      END numerator_flag3
    FROM dsrip_tr018_bp_results r
    JOIN patient_dimension p 
      ON p.patient_id = r.patient_id AND p.network=r.network AND current_flag=1  AND p.date_of_death IS NULL--and r.patient_id=2388362 
    LEFT JOIN pt008.patient_secondary_stage psn
      ON psn.patient_id=r.patient_id AND psn.facility_id=r.facility_id AND psn.network=r.network and psn.visit_id=r.visit_id
    
    --Diabetes Inclusion
    LEFT JOIN 
    (
      SELECT 
        g.patient_id,
        g.network,
        MAX(g.onset_date) onset_date,
        MAX(g.diag_code) diag_code
      FROM  
        (
          SELECT 
            cmv.patient_id,
            cmv.network,
            cmv.onset_date,
            cmv.diag_code
          FROM patient_diag_dimension cmv
          JOIN meta_conditions mc
            ON mc.value = cmv.diag_code AND mc.criterion_id = 37 AND mc.include_exclude_ind = 'I' 
          WHERE NOT EXISTS
          (
            SELECT 
              DISTINCT 
              cmv1.patient_id,cmv1.network
            FROM patient_diag_dimension cmv1
            JOIN meta_conditions mc1 ON mc1.value=cmv1.diag_code AND mc1.criterion_id = 37 AND mc1.include_exclude_ind = 'E'
            WHERE cmv1.patient_id=cmv.patient_id and cmv1.network=cmv.network   
          )  
          UNION
          SELECT 
            DISTINCT a.patient_id, 
            network, 
            null onset_date, 
            null diag_code
          FROM fact_prescriptions a
          JOIN ref_drug_descriptions b
            ON a.drug_description = b.drug_description
            AND b.drug_type_id = 33
      )g
      GROUP BY g.network,g.patient_id 
    )diab_prob_pat 
    ON diab_prob_pat.patient_id = r.patient_id and diab_prob_pat.network=r.network
    JOIN facility_dimension f
      ON f.facility_id=r.facility_id AND f.network = r.network
    LEFT JOIN pt008.payer_mapping pm
      ON pm.payer_id=r.payer_id AND pm.network=r.network
    WHERE FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 18 and 85
  )
SELECT --+ parallel(16)
  network, 
  facility_name,         
  patient_id, 
  patient_name, 
  mrn,
  dob birthdate, 
  apt_suite,		
  street_address,	
  city,			
  state,			
  country,			
  mailing_code,	
  home_phone,
  age,         
  visit_type_name,       
  visit_id,
  clinic_code,
  clinic_code_service,
  clinic_code_desc,    
  admission_date_time,
  discharge_date_time,    
  payer_group,
  payer_name, 
  AGE_18_59,
  AGE_60_85,
  diabetic,  
  diabetes_dx_code,
  htn_dx_code hypertension_dx_code,
  onset_date hypertension_onset_date,   
  bp_reading_time,       
  systolic_bp,           
  diastolic_bp,          
  numerator_flag1,
  numerator_flag2,
  numerator_flag3
FROM pat_bp_results;
select * from dsrip_tr018_bp_results;
select * from patient_dimension;