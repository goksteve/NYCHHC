exec dbm.drop_tables('DSRIP_REPORT_PQI90');

CREATE TABLE DSRIP_REPORT_PQI90
(
  report_period_start_dt         DATE,
  network                        CHAR(3 BYTE)   NOT NULL,
  facility                       VARCHAR2(100 BYTE),
  last_name                      VARCHAR2(400 BYTE),
  first_name                     VARCHAR2(400 BYTE),
  dob                            DATE,
  mrn                            VARCHAR2(40 CHAR),
  street_address                 VARCHAR2(1024 BYTE),
  apt_suite                      VARCHAR2(1024 BYTE),
  city                           VARCHAR2(50 BYTE),
  state                          VARCHAR2(50 BYTE),
  country                        VARCHAR2(50 BYTE),
  zip_code                       VARCHAR2(50 BYTE),  
  home_phone                     VARCHAR2(50 BYTE),
  cell_phone                     VARCHAR2(50 BYTE),
  visit_id                       NUMBER(12)     NOT NULL,
  visit_number                   VARCHAR2(40 BYTE),
  admission_dt                   DATE,
  discharge_dt                   DATE,
  fin_class                      VARCHAR2(100 CHAR),
  payer_type                     VARCHAR2(4000 CHAR),
  payer_name                     VARCHAR2(150 BYTE),
  prim_care_provider             VARCHAR2(60 BYTE),
  attending_provider             VARCHAR2(60 CHAR),
  resident_provider              VARCHAR2(60 CHAR),
  pcp_visit_id                   NUMBER(12),
  pcp_visit_dt                   DATE,  
  pcp_vst_facility_name          VARCHAR2(100 BYTE),
  diab_shortterm_diagnoses       VARCHAR2(2000 CHAR),
  diab_shortterm_exclusion       VARCHAR2(2000 CHAR),  
  diab_longterm_diagnoses        VARCHAR2(2000 CHAR),
  diab_longterm_exclusion        VARCHAR2(2000 CHAR),        
  copd_asthma_adults_diagnoses   VARCHAR2(2000 CHAR),
  copd_asthma_adults_exclusion   VARCHAR2(2000 CHAR),
  hypertension_diagnoses         VARCHAR2(2000 CHAR),
  hypertension_exclusion         VARCHAR2(2000 CHAR),
  heart_failure_diagnoses        VARCHAR2(2000 CHAR),
  heart_failure_exclusion        VARCHAR2(2000 CHAR),
  dehydration_diagnoses          VARCHAR2(2000 CHAR),
  dehydration_exclusion          VARCHAR2(2000 CHAR),
  bacterial_pneumonia_diagnoses  VARCHAR2(2000 CHAR),
  bacterial_pneumonia_exclusion  VARCHAR2(2000 CHAR),
  urinary_tract_inf_diagnoses    VARCHAR2(2000 CHAR),
  urinary_tract_inf_exclusion    VARCHAR2(2000 CHAR),
  uncontrolled_diab_diagnoses    VARCHAR2(2000 CHAR),
  uncontrolled_diab_exclusion    VARCHAR2(2000 CHAR),
  asthma_yng_adlt_diagnoses      VARCHAR2(2000 CHAR),
  asthma_yng_adlt_exclusion      VARCHAR2(2000 CHAR),
  amputation_diab_diagnoses      VARCHAR2(2000 CHAR),
  amputation_diab_exclusion      VARCHAR2(2000 CHAR),
  CONSTRAINT dsrip_report_pqi90 PRIMARY KEY(report_period_start_dt, network, visit_id) USING INDEX COMPRESS
)
COMPRESS BASIC;
