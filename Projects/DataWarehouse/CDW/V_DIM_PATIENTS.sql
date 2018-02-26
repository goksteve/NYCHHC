CREATE OR REPLACE VIEW v_dim_patients AS
SELECT
  p.network,
  p.patient_id,
  d.patient_key,
  d.archive_number,
  pcp.emp_provider_id,
  mep.name emp_provider_name,
  p.name,
  p.title_id,
  p.medical_record_number,
  p.sex,
  p.birthdate,
  p.date_of_death,
  p.apt_suite,
  p.street_address,
  p.city,
  p.state,
  p.country,
  p.mailing_code,
  p.marital_status_id,
  ms.name AS marital_status_desc,
  p.race_id,
  mr.name AS race_desc,
  p.religion_id,
  rel.name AS religion_desc,
  p.free_text_religion,
  p.free_text_occupation,
  p.free_text_employer,
  p.mother_patient_id,
  p.collapsed_into_patient_id,
  p.social_security_number,
  p.lifecare_visit_id,
  p.confidential_flag,
  NVL(p.home_phone, 'NA') AS home_phone,
  NVL(p.day_phone, 'NA') AS day_phone,
  p.smoker_flag,
  p.current_location,
  NVL(p.sec_lang_name, 'NA') AS sec_lang_name,
  p.addr_string,
  NVL(p.block_code, 'NA') AS block_code,
  p.last_edit_time,
  p.county,
  p.sub_building_name,
  p.building_name,
  p.building_nbr,
  p.dependent_street,
  p.dependent_locality,
  p.head_of_house_patient_id
FROM patient p
LEFT JOIN marital_status ms ON p.marital_status_id = ms.marital_status_id AND p.network = ms.network
LEFT JOIN race mr ON p.race_id = mr.race_id AND p.network = mr.network
LEFT JOIN religion rel ON p.religion_id = rel.religion_id AND p.network = rel.network
LEFT JOIN occupation mo ON p.occupation_id = mo.occupation_id AND p.network = mo.network
LEFT JOIN patient_care_provider pcp ON pcp.patient_id = p.patient_id AND pcp.network = p.network AND pcp.patient_care_provider_number = 1 AND pcp.relationship_id = 1
LEFT JOIN emp_provider mep ON mep.emp_provider_id = pcp.emp_provider_id AND mep.network = pcp.network
LEFT JOIN dim_patients d ON d.patient_id = p.patient_id AND d.network = p.network AND d.current_flag = 1   
WHERE LOWER(p.name) NOT LIKE 'test,%' AND LOWER(p.name) NOT LIKE 'testing,%' AND LOWER(p.name) NOT LIKE '%,test' AND LOWER(p.name) NOT LIKE 'testggg,%' 
AND LOWER(p.name) NOT LIKE '%,test%ccd' AND LOWER(p.name) NOT LIKE 'test%ccd,%' AND LOWER(p.name) <> 'emergency,testone' AND LOWER(p.name) <> 'testtwo,testtwo'
AND d.patient_id IS NULL OR
(
  t.PATIENT_KEY <> q.PATIENT_KEY 
  t.NETWORK <> q.NETWORK
  t.PATIENT_ID <> q.PATIENT_ID
  NVL(t.ARCHIVE_NUMBER, -101010101) = NVL(q.ARCHIVE_NUMBER, -101010101)
  NVL(t.NAME, '$$N/A$$') = NVL(q.NAME, '$$N/A$$')
  NVL(t.PCP_PROVIDER_ID, -101010101) = NVL(q.PCP_PROVIDER_ID, -101010101)
  NVL(t.PCP_PROVIDER_NAME, '$$N/A$$') = NVL(q.PCP_PROVIDER_NAME, '$$N/A$$')
  NVL(t.TITLE_ID, -101010101) = NVL(q.TITLE_ID, -101010101)
  NVL(t.MEDICAL_RECORD_NUMBER, '$$N/A$$') = NVL(q.MEDICAL_RECORD_NUMBER, '$$N/A$$')
  NVL(t.SEX, '$$N/A$$') = NVL(q.SEX, '$$N/A$$')
  NVL(t.BIRTHDATE, DATE '0001-01-01') = NVL(q.BIRTHDATE, DATE '0001-01-01')
  NVL(t.DATE_OF_DEATH, DATE '0001-01-01') = NVL(q.DATE_OF_DEATH, DATE '0001-01-01')
  NVL(t.APT_SUITE, '$$N/A$$') = NVL(q.APT_SUITE, '$$N/A$$')
  NVL(t.STREET_ADDRESS, '$$N/A$$') = NVL(q.STREET_ADDRESS, '$$N/A$$')
  NVL(t.CITY, '$$N/A$$') = NVL(q.CITY, '$$N/A$$')
  NVL(t.STATE, '$$N/A$$') = NVL(q.STATE, '$$N/A$$')
  NVL(t.COUNTRY, '$$N/A$$') = NVL(q.COUNTRY, '$$N/A$$')
  NVL(t.MAILING_CODE, '$$N/A$$') = NVL(q.MAILING_CODE, '$$N/A$$')
  NVL(t.MARITAL_STATUS_ID, -101010101) = NVL(q.MARITAL_STATUS_ID, -101010101)
  NVL(t.MARITAL_STATUS_DESC, '$$N/A$$') = NVL(q.MARITAL_STATUS_DESC, '$$N/A$$')
  NVL(t.RACE_ID, -101010101) = NVL(q.RACE_ID, -101010101)
  NVL(t.RACE_DESC, '$$N/A$$') = NVL(q.RACE_DESC, '$$N/A$$')
  NVL(t.RELIGION_ID, -101010101) = NVL(q.RELIGION_ID, -101010101)
  NVL(t.RELIGION_DESC, '$$N/A$$') = NVL(q.RELIGION_DESC, '$$N/A$$')
  NVL(t.FREE_TEXT_RELIGION, '$$N/A$$') = NVL(q.FREE_TEXT_RELIGION, '$$N/A$$')
  NVL(t.FREE_TEXT_OCCUPATION, '$$N/A$$') = NVL(q.FREE_TEXT_OCCUPATION, '$$N/A$$')
  NVL(t.FREE_TEXT_EMPLOYER, '$$N/A$$') = NVL(q.FREE_TEXT_EMPLOYER, '$$N/A$$')
  NVL(t.MOTHER_PATIENT_ID, -101010101) = NVL(q.MOTHER_PATIENT_ID, -101010101)
  NVL(t.COLLAPSED_INTO_PATIENT_ID, -101010101) = NVL(q.COLLAPSED_INTO_PATIENT_ID, -101010101)
  NVL(t.SOCIAL_SECURITY_NUMBER, '$$N/A$$') = NVL(q.SOCIAL_SECURITY_NUMBER, '$$N/A$$')
  NVL(t.LIFECARE_VISIT_ID, -101010101) = NVL(q.LIFECARE_VISIT_ID, -101010101)
  NVL(t.CONFIDENTIAL_FLAG, '$$N/A$$') = NVL(q.CONFIDENTIAL_FLAG, '$$N/A$$')
  NVL(t.HOME_PHONE, '$$N/A$$') = NVL(q.HOME_PHONE, '$$N/A$$')
  NVL(t.DAY_PHONE, '$$N/A$$') = NVL(q.DAY_PHONE, '$$N/A$$')
  NVL(t.SMOKER_FLAG, '$$N/A$$') = NVL(q.SMOKER_FLAG, '$$N/A$$')
  NVL(t.CURRENT_LOCATION, '$$N/A$$') = NVL(q.CURRENT_LOCATION, '$$N/A$$')
  NVL(t.SEC_LANG_NAME, '$$N/A$$') = NVL(q.SEC_LANG_NAME, '$$N/A$$')
  NVL(t.ADDR_STRING, '$$N/A$$') = NVL(q.ADDR_STRING, '$$N/A$$')
  NVL(t.BLOCK_CODE, '$$N/A$$') = NVL(q.BLOCK_CODE, '$$N/A$$')
  NVL(t.LAST_EDIT_TIME, DATE '0001-01-01') = NVL(q.LAST_EDIT_TIME, DATE '0001-01-01')
  NVL(t.COUNTY, '$$N/A$$') = NVL(q.COUNTY, '$$N/A$$')
  NVL(t.SUB_BUILDING_NAME, '$$N/A$$') = NVL(q.SUB_BUILDING_NAME, '$$N/A$$')
  NVL(t.BUILDING_NAME, '$$N/A$$') = NVL(q.BUILDING_NAME, '$$N/A$$')
  NVL(t.BUILDING_NBR, '$$N/A$$') = NVL(q.BUILDING_NBR, '$$N/A$$')
  NVL(t.HEAD_OF_HOUSE_PATIENT_ID, -101010101) = NVL(q.HEAD_OF_HOUSE_PATIENT_ID, -101010101)
);
