CREATE OR REPLACE VIEW V_DIM_PATIENTS_FULL AS
 WITH patient_archive_stage AS
		  (
 SELECT --+ materialize
  network,
  patient_id,
  archive_number,
  archive_time,
  emp_provider_id,
  name,
  title_id,
  medical_record_number,
  sex,
  birthdate,
  date_of_death,
  apt_suite,
  street_address,
  city,
  state,
  country,
  mailing_code,
  marital_status_id,
  race_id,
  religion_id,
  free_text_religion,
  occupation_id,
  free_text_occupation,
  employer_id,
  free_text_employer,
  mother_patient_id,
  collapsed_into_patient_id,
  social_security_number,
  confidential_flag,
  home_phone,
  day_phone,
  smoker_flag,
  sec_lang_name,
  addr_string,
  block_code,
  county,
  sub_building_name,
  building_name,
  building_nbr,
  dependent_street,
  dependent_locality,
  uk_nhs_number,
  uk_ha_res_code,
  uk_pct_res_code,
  head_of_house_patient_id,
  patient_archive_type_id,
  archive_source_id,
  ROW_NUMBER()	OVER(PARTITION BY network, patient_id, TRUNC(archive_time, 'HH12') ORDER BY
          network,
          patient_id ASC,
          archive_number DESC,
          archive_time DESC)
    row_num
FROM patient_archive pa
WHERE 	 1 = 1
  AND archive_time IS NOT NULL
  AND TRIM(
         DECODE(emp_provider_id, CHR(127), NULL, emp_provider_id)
       || DECODE(name, CHR(127), NULL, name)
       || DECODE(title_id, CHR(127), NULL, title_id)
       || DECODE(medical_record_number, CHR(127), NULL, medical_record_number)
       || DECODE(sex, CHR(127), NULL, sex)
       || DECODE(birthdate, CHR(127), NULL, birthdate)
       || DECODE(date_of_death, CHR(127), NULL, date_of_death)
       || DECODE(apt_suite, CHR(127), NULL, apt_suite)
       || DECODE(street_address, CHR(127), NULL, street_address)
       || DECODE(city, CHR(127), NULL, city)
       || DECODE(state, CHR(127), NULL, state)
       || DECODE(country, CHR(127), NULL, country)
       || DECODE(mailing_code, CHR(127), NULL, mailing_code)
       || DECODE(REPLACE(marital_status_id, -99, NULL),
              NULL, NULL,
              CHR(127), NULL,
              marital_status_id)
       || DECODE(REPLACE(race_id, -99, NULL),  NULL, NULL,	CHR(127), NULL,  race_id)
       || DECODE(REPLACE(religion_id, -99, NULL),
              NULL, NULL,
              CHR(127), NULL,
              religion_id)
       || DECODE(free_text_religion,  CHR(127), NULL,  NULL, NULL,  free_text_religion)
       || DECODE(REPLACE(occupation_id, -99, NULL),
              NULL, NULL,
              CHR(127), NULL,
              occupation_id)
       || DECODE(free_text_occupation,  CHR(127), NULL,	NULL, NULL,  free_text_occupation)
       || DECODE(REPLACE(employer_id, -99, NULL),
              NULL, NULL,
              CHR(127), NULL,
              employer_id)
       || DECODE(free_text_employer, CHR(127), NULL, free_text_employer)
       || DECODE(REPLACE(mother_patient_id, -99),
              NULL, NULL,
              CHR(127), NULL,
              mother_patient_id)
       || DECODE(REPLACE(collapsed_into_patient_id, -99, NULL),
              NULL, NULL,
              CHR(127), NULL,
              collapsed_into_patient_id)
       || DECODE(social_security_number, CHR(127), NULL, social_security_number)
       || DECODE(confidential_flag, CHR(127), NULL, confidential_flag)
       || DECODE(home_phone, CHR(127), NULL, home_phone)
       || DECODE(day_phone, CHR(127), NULL, day_phone)
       || DECODE(smoker_flag, CHR(127), NULL, smoker_flag)
       || DECODE(sec_lang_name, CHR(127), NULL, sec_lang_name)
       || DECODE(addr_string, CHR(127), NULL, addr_string)
       || DECODE(block_code, CHR(127), NULL, block_code)
       || DECODE(county, CHR(127), NULL, county)
       || DECODE(sub_building_name, CHR(127), NULL, sub_building_name)
       || DECODE(building_name, CHR(127), NULL, building_name)
       || DECODE(building_nbr, CHR(127), NULL, building_nbr)
       || DECODE(dependent_street, CHR(127), NULL, dependent_street)
       || DECODE(dependent_locality, CHR(127), NULL, dependent_locality)
       || DECODE(REPLACE(uk_nhs_number, -99, NULL),
              NULL, NULL,
              CHR(127), NULL,
              uk_nhs_number)
       || DECODE(uk_ha_res_code, CHR(127), NULL, uk_ha_res_code)
       || DECODE(uk_pct_res_code, CHR(127), NULL, uk_pct_res_code)
       || DECODE(head_of_house_patient_id, CHR(127), NULL, head_of_house_patient_id))
       IS NOT NULL
  AND LOWER(pa.name) NOT LIKE 'test,%'
  AND LOWER(pa.name) NOT LIKE 'testing,%'
  AND LOWER(pa.name) NOT LIKE '%,test'
  AND LOWER(pa.name) NOT LIKE 'testggg,%'
  AND LOWER(pa.name) NOT LIKE '%,test%ccd'
  AND LOWER(pa.name) NOT LIKE 'test%ccd,%'
  AND LOWER(pa.name) <> 'emergency,testone'
  AND LOWER(pa.name) <> 'testtwo,testtwo'
  AND EXISTS
       (SELECT 1
         FROM patient pp
        WHERE pp.patient_id = pa.patient_id AND pp.network = pa.network)
) 
--pat		 WHERE row_num = 1)
,
pat_comb
AS
(
	SELECT /*+ PARALLEL(32) */
			network,
			-- SEQ_DIM_PATIENTS.NEXTVAL AS patient_key,
			 patient_id,
			 archive_number,
			 archive_time,
			 NVL(emp_provider_id, -99) AS pcp_provider_id,
			 NVL(emp_provider_name, 'NA') AS pcp_provider_name,
			 name,
			 title_id,
			 medical_record_number,
			 DECODE(sex, CHR(127), NULL, sex) AS sex,
			 DECODE(birthdate, CHR(127), NULL, birthdate) AS birthdate,
			 DECODE(date_of_death, CHR(127), NULL, date_of_death) AS date_of_death,
			 DECODE(apt_suite, CHR(127), NULL, apt_suite) AS apt_suite,
			 DECODE(street_address, CHR(127), NULL, street_address) AS street_address,
			 DECODE(city, CHR(127), NULL, city) AS city,
			 DECODE(state, CHR(127), NULL, state) AS state,
			 DECODE(country, CHR(127), NULL, country) AS country,
			 DECODE(mailing_code, CHR(127), NULL, mailing_code) AS mailing_code,
			 DECODE(marital_status_id, CHR(127), NULL, marital_status_id) AS marital_status_id,
			 marital_status_desc,
			 DECODE(race_id, CHR(127), NULL, race_id) AS race_id,
			 race_desc,
			 DECODE(religion_id, CHR(127), NULL, religion_id) AS religion_id,
			 religion_desc,
			 DECODE(free_text_religion, CHR(127), NULL, free_text_religion) AS free_text_religion,
			 DECODE(occupation_id, CHR(127), NULL, occupation_id) AS occupation_id,
			 occupation_desc,
			 DECODE(free_text_occupation, CHR(127), NULL, free_text_occupation) AS free_text_occupation,
			 DECODE(employer_id, CHR(127), NULL, employer_id) AS employer_id,
			 employer_name,
			 DECODE(free_text_employer, CHR(127), NULL, free_text_employer) AS free_text_employer,
			 DECODE(mother_patient_id, CHR(127), NULL, mother_patient_id) AS mother_patient_id,
			 DECODE(collapsed_into_patient_id, CHR(127), NULL, collapsed_into_patient_id) AS collapsed_into_patient_id,
			 DECODE(NVL(social_security_number, 'NA'), CHR(127), NULL, social_security_number) AS social_security_number,
			 lifecare_visit_id,
			 DECODE(confidential_flag, CHR(127), NULL, confidential_flag) AS confidential_flag,
			 DECODE(home_phone, CHR(127), NULL, home_phone) AS home_phone,
			 DECODE(day_phone, CHR(127), NULL, day_phone) AS day_phone,
			 DECODE(smoker_flag, CHR(127), NULL, smoker_flag) AS smoker_flag,
			 current_location,
			 DECODE(sec_lang_name, CHR(127), NULL, sec_lang_name) AS sec_lang_name,
			 DECODE(addr_string, CHR(127), NULL, addr_string) AS addr_string,
			 DECODE(block_code, CHR(127), NULL, block_code) AS block_code,
			 last_edit_time,
			 DECODE(county, CHR(127), NULL, county) AS county,
			 DECODE(sub_building_name, CHR(127), NULL, sub_building_name) AS sub_building_name,
			 DECODE(building_name, CHR(127), NULL, building_name) AS building_name,
			 DECODE(building_nbr, CHR(127), NULL, building_nbr) AS building_nbr,
			 DECODE(dependent_street, CHR(127), NULL, dependent_street) AS dependent_street,
			 DECODE(dependent_locality, CHR(127), NULL, dependent_locality) AS dependent_locality,
			 DECODE(uk_nhs_number, CHR(127), NULL, uk_nhs_number) AS uk_nhs_number,
			 DECODE(uk_ha_res_code, CHR(127), NULL, uk_ha_res_code) AS uk_ha_res_code,
			 DECODE(uk_pct_res_code, CHR(127), NULL, uk_pct_res_code) AS uk_pct_res_code,
			 DECODE(head_of_house_patient_id, CHR(127), NULL, head_of_house_patient_id) AS head_of_house_patient_id,
			 DECODE(patient_archive_type_id, CHR(127), NULL, patient_archive_type_id) AS patient_archive_type_id,
			 DECODE(archive_source_id, CHR(127), NULL, archive_source_id) AS archive_source_id,
			 current_flag,
			 effective_from,
			 effective_to
	  FROM (SELECT p.network,
						p.patient_id,
						-99 AS archive_number,
						DATE '1900-01-01' AS archive_time,
						pcp.emp_provider_id,
						pcp.emp_provider_name,
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
						p.occupation_id,
						mo.name AS occupation_desc,
						p.free_text_occupation,
						p.employer_id,
						mem.name AS employer_name,
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
						p.uk_nhs_number,
						p.uk_ha_res_code,
						p.uk_pct_res_code,
						p.head_of_house_patient_id,
						-99 AS patient_archive_type_id,
						-99 archive_source_id,
						1 AS current_flag,
						DATE '1900-01-01' AS effective_from,
						DATE '1900-01-01' AS effective_to
				 FROM patient p
						LEFT JOIN marital_status ms ON p.marital_status_id = ms.marital_status_id AND p.network = ms.network
						LEFT JOIN race mr ON p.race_id = mr.race_id AND p.network = mr.network
						LEFT JOIN religion rel ON p.religion_id = rel.religion_id AND p.network = rel.network
						LEFT JOIN occupation mo ON p.occupation_id = mo.occupation_id AND p.network = mo.network
						LEFT JOIN employer mem ON p.employer_id = mem.employer_id AND p.network = mem.network
						LEFT JOIN
						(SELECT pcp.network,
								  patient_id,
								  pcp.emp_provider_id,
								  mep.name AS emp_provider_name
							FROM patient_care_provider pcp
								  LEFT JOIN emp_provider mep
									  ON pcp.emp_provider_id = mep.emp_provider_id AND pcp.network = mep.network
						  WHERE pcp.relationship_id = 1 AND pcp.patient_care_provider_number = 1) pcp
							ON p.patient_id = pcp.patient_id AND p.network = pcp.network
				WHERE 	 1 = 1
						AND ( 	LOWER(p.name) NOT LIKE 'test,%'
							  AND LOWER(p.name) NOT LIKE 'testing,%'
							  AND LOWER(p.name) NOT LIKE '%,test'
							  AND LOWER(p.name) NOT LIKE 'testggg,%'
							  AND LOWER(p.name) NOT LIKE '%,test%ccd'
							  AND LOWER(p.name) NOT LIKE 'test%ccd,%'
							  AND LOWER(p.name) <> 'emergency,testone'
							  AND LOWER(p.name) <> 'testtwo,testtwo')
			  UNION
			  SELECT /*+ PARALLEL (32) */
					  p.network,
						p.patient_id,
						p.archive_number,
						p.archive_time,
						p.emp_provider_id,
						mep.name AS emp_provider_name,
						p.name,
						p.title_id,
						p.medical_record_number,
						NVL(p.sex, 'NA') AS sex,
						p.birthdate,
						p.date_of_death,
						p.apt_suite,
						NVL(p.street_address, 'NA'),
						NVL(p.city, 'NA') AS city,
						NVL(p.state, 'Unknown') AS state,
						NVL(p.country, 'Unknown') AS country,
						NVL(p.mailing_code, 'NA') AS mailing_code,
						p.marital_status_id,
						ms.name AS marital_status_desc,
						p.race_id,
						mr.name AS race_desc,
						p.religion_id,
						rel.name AS religion_desc,
						free_text_religion,
						p.occupation_id,
						mo.name AS occupation_desc,
						free_text_occupation,
						p.employer_id,
						mem.name AS employer_name,
						free_text_employer,
						mother_patient_id,
						collapsed_into_patient_id,
						NVL(p.social_security_number, 'NA') AS social_security_number,
						NULL AS lifecare_visit_id,
						confidential_flag,
						NVL(p.home_phone, 'NA') AS home_phone,
						NVL(p.day_phone, 'NA') AS day_phone,
						smoker_flag,
						'NA' AS current_location,
						NVL(p.sec_lang_name, 'NA') AS sec_lang_name,
						addr_string,
						NVL(p.block_code, 'NA') AS block_code,
						DATE '1900-01-01' AS last_edit_time,
						county,
						sub_building_name,
						building_name,
						building_nbr,
						dependent_street,
						dependent_locality,
						uk_nhs_number,
						uk_ha_res_code,
						uk_pct_res_code,
						head_of_house_patient_id,
						patient_archive_type_id,
						archive_source_id,
						0 AS current_flag,
						archive_time AS effective_from,
						NVL(LEAD(archive_time, 1) OVER(PARTITION BY patient_id ORDER BY patient_id ASC, archive_number ASC),
							 DATE '9999-12-31')
							AS effective_to
				 FROM patient_archive_stage p
						LEFT JOIN marital_status ms ON p.marital_status_id = ms.marital_status_id AND p.network = ms.network
						LEFT JOIN race mr ON p.race_id = mr.race_id AND p.network = mr.network
						LEFT JOIN religion rel ON p.religion_id = rel.religion_id AND p.network = rel.network
						LEFT JOIN occupation mo ON p.occupation_id = mo.occupation_id AND p.network = mo.network
						LEFT JOIN employer mem ON p.employer_id = mem.employer_id AND p.network = mem.network
						LEFT JOIN emp_provider mep ON p.emp_provider_id = mep.emp_provider_id AND p.network = mep.network
            WHERE  p.row_num = 1)
)

 SELECT
  
  network,
--SEQ_DIM_PATIENTS.NEXTVAL AS patient_key,
  patient_id,
CASE
   WHEN archive_number IS NULL THEN
    NVL(LAG(archive_number) OVER(PARTITION BY network, patient_id ORDER BY archive_number NULLS LAST),0) + 1
   ELSE
    archive_number
  END
   archive_number,
  name,
  pcp_provider_id,
  pcp_provider_name,
  title_id,
  medical_record_number,
  sex,
  birthdate,
  date_of_death,
  apt_suite,
  street_address,
  city,
  state,
  country,
  mailing_code,
  marital_status_id,
  marital_status_desc,
  race_id,
  race_desc,
  religion_id,
  religion_desc,
  free_text_religion,
  free_text_occupation,
  free_text_employer,
  mother_patient_id,
  collapsed_into_patient_id,
  social_security_number,
  lifecare_visit_id,
  confidential_flag,
  home_phone,
  day_phone,
  smoker_flag,
  current_location,
  sec_lang_name,
  addr_string,
  block_code,
  last_edit_time,
  county,
  sub_building_name,
  building_name,
  building_nbr,
  head_of_house_patient_id,
  current_flag,
  CASE
   WHEN archive_number = 1 THEN DATE '1901-01-01'
   ELSE NVL(LAG(effective_from) OVER(PARTITION BY network, patient_id ORDER BY archive_number NULLS LAST), DATE '1901-01-01')
  END
   effective_from,
  CASE WHEN archive_number IS NULL THEN DATE '9999-12-31' ELSE effective_from END effedtive_to 
  
 FROM
  pat_comb;