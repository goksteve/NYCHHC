CREATE OR REPLACE VIEW v_dim_patients_stg AS
SELECT * FROM v_dim_patients WHERE ROWNUM < 1;

CREATE OR REPLACE TRIGGER tr_v_dim_patients_stg
INSTEAD OF INSERT ON v_dim_patients_stg FOR EACH ROW
BEGIN
  IF :new.change <> 'NEW' THEN
    UPDATE dim_patients SET effective_to = SYSDATE, current_flag = 0
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
