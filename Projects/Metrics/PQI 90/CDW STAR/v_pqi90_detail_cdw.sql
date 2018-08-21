CREATE OR REPLACE VIEW v_pqi90_detail_cdw 
AS 
SELECT
 -- 05-Jul-2018, GK: Added report_dt to the view
 -- 05-Apr-2018, GK: created
  report_period_start_dt AS report_dt,
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
  CASE
  WHEN FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - rpt.dob)/365) BETWEEN 18 AND 39 
  THEN '18 - 39 Years'
  WHEN FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - rpt.dob)/365) > 39 
  THEN '40+ Years'
  END AS age_group,
  network,
  last_name,
  first_name,
  dob,
  FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - rpt.dob)/365) age,
  street_address,
  apt_suite,
  city,
  state,
  country,
  zip_code,
  home_phone,
  cell_phone,
  admission_dt,
  discharge_dt,
  mrn,
  visit_id
  visit_number,
  facility,
  attending_provider,
  fin_class,  
  payer_type,
  payer_name,
  prim_care_provider,
  pcp_visit_id,
  pcp_visit_dt,
  pcp_vst_facility_name,
  CASE WHEN diab_shortterm_diagnoses LIKE '%--%' THEN '"'||diab_shortterm_diagnoses||'"' ELSE diab_shortterm_diagnoses END diab_shortterm_diagnoses,
  CASE WHEN diab_shortterm_exclusion LIKE '%--%' THEN '"'||diab_shortterm_exclusion||'"' ELSE diab_shortterm_exclusion END diab_shortterm_exclusion,
  CASE WHEN diab_longterm_diagnoses LIKE '%--%' THEN '"'||diab_longterm_diagnoses||'"' ELSE diab_longterm_diagnoses END diab_longterm_diagnoses,
  CASE WHEN diab_longterm_exclusion LIKE '%--%' THEN '"'||diab_longterm_exclusion||'"' ELSE diab_longterm_exclusion END diab_longterm_exclusion,
  CASE WHEN copd_asthma_adults_diagnoses LIKE '%--%' THEN '"'||copd_asthma_adults_diagnoses||'"' ELSE copd_asthma_adults_diagnoses END copd_asthma_adults_diagnoses,
  CASE WHEN copd_asthma_adults_exclusion LIKE '%--%' THEN '"'||copd_asthma_adults_exclusion||'"' ELSE copd_asthma_adults_exclusion END copd_asthma_adults_exclusion,
  CASE WHEN hypertension_diagnoses LIKE '%--%' THEN '"'||hypertension_diagnoses||'"' ELSE hypertension_diagnoses END hypertension_diagnoses,
  CASE WHEN hypertension_exclusion LIKE '%--%' THEN '"'||hypertension_exclusion||'"' ELSE hypertension_exclusion END hypertension_exclusion,
  CASE WHEN heart_failure_diagnoses LIKE '%--%' THEN '"'||heart_failure_diagnoses||'"' ELSE heart_failure_diagnoses END heart_failure_diagnoses,
  CASE WHEN heart_failure_exclusion LIKE '%--%' THEN '"'||heart_failure_exclusion||'"' ELSE heart_failure_exclusion END heart_failure_exclusion,
  CASE WHEN dehydration_diagnoses LIKE '%--%' THEN '"'||dehydration_diagnoses||'"' ELSE dehydration_diagnoses END dehydration_diagnoses,
  CASE WHEN dehydration_exclusion LIKE '%--%' THEN '"'||dehydration_exclusion||'"' ELSE dehydration_exclusion END dehydration_exclusion,
  CASE WHEN bacterial_pneumonia_diagnoses LIKE '%--%' THEN '"'||bacterial_pneumonia_diagnoses||'"' ELSE bacterial_pneumonia_diagnoses END bacterial_pneumonia_diagnoses,
  CASE WHEN bacterial_pneumonia_exclusion LIKE '%--%' THEN '"'||bacterial_pneumonia_exclusion||'"' ELSE bacterial_pneumonia_exclusion END bacterial_pneumonia_exclusion,
  CASE WHEN urinary_tract_inf_diagnoses LIKE '%--%' THEN '"'||urinary_tract_inf_diagnoses||'"' ELSE urinary_tract_inf_diagnoses END urinary_tract_inf_diagnoses,
  CASE WHEN urinary_tract_inf_exclusion LIKE '%--%' THEN '"'||urinary_tract_inf_exclusion||'"' ELSE urinary_tract_inf_exclusion END urinary_tract_inf_exclusion,
  CASE WHEN uncontrolled_diab_diagnoses LIKE '%--%' THEN '"'||uncontrolled_diab_diagnoses||'"' ELSE uncontrolled_diab_diagnoses END uncontrolled_diab_diagnoses,
  CASE WHEN uncontrolled_diab_exclusion LIKE '%--%' THEN '"'||uncontrolled_diab_exclusion||'"' ELSE uncontrolled_diab_exclusion END uncontrolled_diab_exclusion,
  CASE WHEN asthma_yng_adlt_diagnoses LIKE '%--%' THEN '"'||asthma_yng_adlt_diagnoses||'"' ELSE asthma_yng_adlt_diagnoses END asthma_yng_adlt_diagnoses,
  CASE WHEN asthma_yng_adlt_exclusion LIKE '%--%' THEN '"'||asthma_yng_adlt_exclusion||'"' ELSE asthma_yng_adlt_exclusion END asthma_yng_adlt_exclusion,
  CASE WHEN amputation_diab_diagnoses LIKE '%--%' THEN '"'||amputation_diab_diagnoses||'"' ELSE amputation_diab_diagnoses END amputation_diab_diagnoses,
  CASE WHEN amputation_diab_exclusion LIKE '%--%' THEN '"'||amputation_diab_exclusion||'"' ELSE amputation_diab_exclusion END amputation_diab_exclusion
FROM dsrip_report_tr006_pqi90 rpt
WHERE report_period_start_dt = NVL(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER'), (SELECT MAX(report_period_start_dt) FROM dsrip_report_tr006_pqi90)) 
ORDER BY last_name, first_name, discharge_dt;