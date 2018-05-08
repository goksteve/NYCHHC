CREATE OR REPLACE VIEW v_pqi90_detail_cdw 
AS 
SELECT
 -- 05-Apr-2018, GK: created
  CASE WHEN diab_shortterm_diagnoses IS NOT NULL AND diab_shortterm_exclusion IS NULL THEN 1 END pqi_01_flag, 
  CASE WHEN diab_longterm_diagnoses IS NOT NULL AND diab_longterm_exclusion IS NULL THEN 1 END pqi_03_flag,
  CASE WHEN copd_asthma_adults_diagnoses IS NOT NULL AND copd_asthma_adults_exclusion IS NULL THEN 1 END pqi_05_flag,
  CASE WHEN hypertension_diagnoses IS NOT NULL AND hypertension_exclusion IS NULL THEN 1 END pqi_07_flag,    
  CASE WHEN heart_failure_diagnoses IS NOT NULL AND heart_failure_exclusion IS NULL THEN 1 END pqi_08_flag,
  CASE WHEN dehydration_diagnoses IS NOT NULL AND dehydration_exclusion IS NULL THEN 1 END pqi_10_flag,
  CASE WHEN bacterial_pneumonia_diagnoses IS NOT NULL AND bacterial_pneumonia_exclusion IS NULL THEN 1 END pqi_11_flag,
  CASE WHEN urinary_tract_inf_diagnoses IS NOT NULL AND urinary_tract_inf_exclusion IS NULL THEN 1 END pqi_12_flag,
  CASE WHEN uncontrolled_diab_diagnoses IS NOT NULL AND uncontrolled_diab_exclusion IS NULL THEN 1 END pqi_14_flag,
  CASE WHEN asthma_yng_adlt_diagnoses IS NOT NULL AND asthma_yng_adlt_exclusion IS NULL THEN 1 END pqi_15_flag,
  CASE WHEN amputation_diab_diagnoses IS NOT NULL AND amputation_diab_exclusion IS NULL THEN 1 END pqi_16_flag,            
--  report_period_start_dt,
--  network,
  CASE
  WHEN FLOOR((add_months(trunc(sysdate,'year'),12)-1 - rpt.dob)/365) BETWEEN 18 AND 39 
  THEN '18 - 39 Years'
  WHEN FLOOR((add_months(trunc(sysdate,'year'),12)-1 - rpt.dob)/365) > 39 
  THEN '40+ Years'
  END AS age_group,
  last_name,
  first_name,
  dob,
  FLOOR((add_months(trunc(sysdate,'year'),12)-1 - rpt.dob)/365) age,
--  address,
--  address2,
--  city,
--  state,
--  country,
--  zip,
--  home_phone,
--  work_phone,
--  cell_phone,
  admission_dt,
  discharge_dt,
  mrn,
--  empi,
  visit_id
  visit_number,
  facility,
--  admitting_provider,??
  attending_provider,
  fin_class,  
--  Payer1,	
--  Payer2,	
--  Payer3
  payer_type,
  payer_name,
  prim_care_provider
--  encounter_type,??
----  primary_diagnosis,
--  CASE WHEN hypertension_diagnoses LIKE '%--%' THEN '"'||hypertension_diagnoses||'"' ELSE hypertension_diagnoses END hypertension_diagnoses,
--  CASE WHEN exclusion_diagnoses LIKE '%--%' THEN '"'||exclusion_diagnoses||'"' ELSE exclusion_diagnoses END exclusion_diagnoses
FROM dsrip_report_pqi90 rpt
WHERE report_period_start_dt = NVL(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER'), (SELECT MAX(report_period_start_dt) FROM dsrip_report_pqi90)) 
ORDER BY last_name, first_name, discharge_dt;