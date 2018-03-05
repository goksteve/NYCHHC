CREATE OR REPLACE VIEW v_dim_patients AS
SELECT
  p.network,
  p.patient_id,
  d.archive_number,
  d.patient_key,
  d.rowid row_id,
  pcp.emp_provider_id AS pcp_provider_id,
  mep.name AS pcp_provider_name,
  p.name,
  p.title_id,
  p.medical_record_number,
  p.sex,
  TRUNC(p.birthdate) birthdate,
  TRUNC(p.date_of_death) date_of_death,
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
  p.head_of_house_patient_id,
  'QCPR' AS source,
  CASE
    WHEN d.patient_id IS NULL THEN 'NEW'
    WHEN NVL(d.name, '$$N/A$$') <> p.name THEN 'NAME'
    WHEN NVL(d.pcp_provider_id, -101010101) <> pcp.emp_provider_id THEN 'PCP_PROVIDER_ID'
    WHEN NVL(d.pcp_provider_name, '$$N/A$$') <> mep.name THEN 'PCP_PROVIDER_NAME'
    WHEN NVL(d.title_id, -101010101) <> p.title_id THEN 'TITLE_ID'
    WHEN NVL(d.medical_record_number, '$$N/A$$') <> p.medical_record_number THEN 'MEDICAL_RECORD_NUMBER'
    WHEN NVL(d.sex, '$$N/A$$') <> p.sex THEN 'SEX' 
    WHEN NVL(d.birthdate, DATE '0001-01-01') <> TRUNC(p.birthdate) THEN 'BIRTHDATE'
    WHEN NVL(d.date_of_death, DATE '0001-01-01') <> TRUNC(p.date_of_death) THEN 'DATE_OF_DEATH'
    WHEN NVL(d.apt_suite, '$$N/A$$') <> p.apt_suite THEN 'APT_SUITE'
    WHEN NVL(d.street_address, '$$N/A$$') <> p.street_address THEN 'STREET_ADDRESS'
    WHEN NVL(d.city, '$$N/A$$') <> p.city THEN 'CITY'
    WHEN NVL(d.state, '$$N/A$$') <> p.state THEN 'STATE'
    WHEN NVL(d.country, '$$N/A$$') <> p.country THEN 'COUNTRY'
    WHEN NVL(d.mailing_code, '$$N/A$$') <> p.mailing_code THEN 'MAILING_CODE'
    WHEN NVL(d.marital_status_id, -101010101) <> p.marital_status_id THEN 'MARITAL_STATUS_ID'
    WHEN NVL(d.marital_status_desc, '$$N/A$$') <> ms.name THEN 'MARITAL_STATUS_DESC'
    WHEN NVL(d.race_id, -101010101) <> p.race_id THEN 'RACE_ID'
    WHEN NVL(d.race_desc, '$$N/A$$') <> mr.name THEN 'RACE_DESC'
    WHEN NVL(d.religion_id, -101010101) <> p.religion_id THEN 'RELIGION_ID'
    WHEN NVL(d.religion_desc, '$$N/A$$') <> rel.name THEN 'RELIGION_DESC'
    WHEN NVL(d.free_text_religion, '$$N/A$$') <> p.free_text_religion THEN 'FREE_TEXT_RELIGION'
    WHEN NVL(d.free_text_occupation, '$$N/A$$') <> p.free_text_occupation THEN 'FREE_TEXT_OCCUPATION'
    WHEN NVL(d.free_text_employer, '$$N/A$$') <> p.free_text_employer THEN 'FREE_TEXT_EMPLOYER'
    WHEN NVL(d.mother_patient_id, -101010101) <> p.mother_patient_id THEN 'MOTHER_PATIENT_ID'
    WHEN NVL(d.collapsed_into_patient_id, -101010101) <> p.collapsed_into_patient_id THEN 'COLLAPSED_INTO_PATIENT_ID'
    WHEN NVL(d.social_security_number, '$$N/A$$') <> p.social_security_number THEN 'SOCIAL_SECURITY_NUMBER'
    WHEN NVL(d.lifecare_visit_id, -101010101) <> p.lifecare_visit_id THEN 'LIFECARE_VISIT_ID'
    WHEN NVL(d.confidential_flag, '$$N/A$$') <> p.confidential_flag THEN 'CONFIDENTIAL_FLAG'
    WHEN NVL(d.home_phone, '$$N/A$$') <> p.home_phone THEN 'HOME_PHONE'
    WHEN NVL(d.day_phone, '$$N/A$$') <> p.day_phone THEN 'DAY_PHONE'
    WHEN NVL(d.smoker_flag, '$$N/A$$') <> p.smoker_flag THEN 'SMOKER_FLAG'
    WHEN NVL(d.current_location, '$$N/A$$') <> p.current_location THEN 'CURRENT_LOCATION'
    WHEN NVL(d.sec_lang_name, '$$N/A$$') <> p.sec_lang_name THEN 'SEC_LANG_NAME'
    WHEN NVL(d.addr_string, '$$N/A$$') <> p.addr_string THEN 'ADDR_STRING'
    WHEN NVL(d.block_code, '$$N/A$$') <> p.block_code THEN 'BLOCK_CODE'
    WHEN NVL(d.county, '$$N/A$$') <> p.county THEN 'COUNTY'
    WHEN NVL(d.sub_building_name, '$$N/A$$') <> p.sub_building_name THEN 'SUB_BUILDING_NAME'
    WHEN NVL(d.building_name, '$$N/A$$') <> p.building_name THEN 'BUILDING_NAME'
    WHEN NVL(d.building_nbr, '$$N/A$$') <> p.building_nbr THEN 'BUILDING_NBR'
    WHEN NVL(d.head_of_house_patient_id, -101010101) <> p.head_of_house_patient_id THEN 'HEAD_OF_HOUSE_PATIENT_ID'
  END change
FROM patient p
LEFT JOIN marital_status ms ON p.marital_status_id = ms.marital_status_id AND p.network = ms.network
LEFT JOIN race mr ON p.race_id = mr.race_id AND p.network = mr.network
LEFT JOIN religion rel ON p.religion_id = rel.religion_id AND p.network = rel.network
LEFT JOIN occupation mo ON p.occupation_id = mo.occupation_id AND p.network = mo.network
LEFT JOIN patient_care_provider pcp
  ON pcp.network = p.network AND pcp.patient_id = p.patient_id
 AND pcp.patient_care_provider_number = 1 AND pcp.relationship_id = 1
LEFT JOIN emp_provider mep
  ON mep.network = pcp.network AND mep.emp_provider_id = pcp.emp_provider_id
LEFT JOIN dim_patients d
  ON d.patient_id = p.patient_id AND d.network = p.network AND d.current_flag = 1   
WHERE LOWER(p.name) NOT LIKE 'test,%' AND LOWER(p.name) NOT LIKE 'testing,%' AND LOWER(p.name) NOT LIKE '%,test' AND LOWER(p.name) NOT LIKE 'testggg,%' 
AND LOWER(p.name) NOT LIKE '%,test%ccd' AND LOWER(p.name) NOT LIKE 'test%ccd,%' AND LOWER(p.name) <> 'emergency,testone' AND LOWER(p.name) <> 'testtwo,testtwo';

CREATE OR REPLACE TRIGGER tr_v_dim_patients
INSTEAD OF INSERT ON v_dim_patients FOR EACH ROW
BEGIN
  IF :new.change <> 'NEW' THEN
    UPDATE dim_patients SET effective_to = TRUNC(SYSDATE), current_flag = 0
    WHERE ROWID = :new.row_id;
  END IF;
  
  INSERT INTO dim_patients
  (
    patient_key, network, patient_id, archive_number, name, 
    pcp_provider_id, pcp_provider_name, title_id, 
    medical_record_number, sex, birthdate, date_of_death, 
    apt_suite, street_address, city, state, country, mailing_code, 
    marital_status_id, marital_status_desc, 
    race_id, race_desc, religion_id, religion_desc, free_text_religion,
    free_text_occupation, free_text_employer, 
    mother_patient_id, collapsed_into_patient_id, social_security_number,
    lifecare_visit_id, confidential_flag, home_phone, day_phone,
    smoker_flag, current_location, sec_lang_name, 
    addr_string, block_code, 
    last_edit_time, county, sub_building_name, building_name, building_nbr,
    head_of_house_patient_id, 
    current_flag, effective_from, effective_to, source
  )
  VALUES
  (
    seq_dim_patients.NEXTVAL, 
    :new.network, :new.patient_id, NVL(:new.archive_number, 0) + 1, :new.name,
    :new.pcp_provider_id, :new.pcp_provider_name, :new.title_id,
    :new.medical_record_number, :new.sex, :new.birthdate, :new.date_of_death,
    :new.apt_suite, :new.street_address, :new.city, :new.state, :new.country, :new.mailing_code,
    :new.marital_status_id, :new.marital_status_desc,
    :new.race_id, :new.race_desc, :new.religion_id, :new.religion_desc, :new.free_text_religion,
    :new.free_text_occupation, :new.free_text_employer,
    :new.mother_patient_id, :new.collapsed_into_patient_id, :new.social_security_number,
    :new.lifecare_visit_id, :new.confidential_flag, :new.home_phone, :new.day_phone,
    :new.smoker_flag, :new.current_location, :new.sec_lang_name,
    :new.addr_string, :new.block_code,
    :new.last_edit_time, :new.county, :new.sub_building_name, :new.building_name, :new.building_nbr,
    :new.head_of_house_patient_id,
    1, TRUNC(SYSDATE), DATE '9999-12-31', :new.source
  );
END;
/
