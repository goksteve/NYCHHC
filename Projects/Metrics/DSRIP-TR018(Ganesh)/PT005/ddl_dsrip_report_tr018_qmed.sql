exec dbm.drop_tables('DSRIP_REPORT_TR018_QMED');

CREATE TABLE dsrip_report_tr018_qmed
(
  network                  VARCHAR2(3 BYTE),
  facility_name            VARCHAR2(100 BYTE),
  patient_id               NUMBER(12),
  patient_name             VARCHAR2(100 BYTE),
  mrn                      VARCHAR2(512 BYTE),
  birthdate                DATE,
  apt_suite                VARCHAR2(1024 BYTE),
  street_address           VARCHAR2(1024 BYTE),
  city                     VARCHAR2(50 BYTE),
  state                    VARCHAR2(50 BYTE),
  country                  VARCHAR2(50 BYTE),
  mailing_code             VARCHAR2(50 BYTE),
  home_phone               VARCHAR2(50 BYTE),
  age                      NUMBER,
  visit_type_name          VARCHAR2(50 BYTE),
  visit_id                 NUMBER,
  clinic_code              VARCHAR2(4000 BYTE),
  clinic_code_service      VARCHAR2(4000 BYTE),
  clinic_code_desc         VARCHAR2(4000 BYTE),
  admission_date_time      DATE,
  discharge_date_time      DATE,
  payer_group              VARCHAR2(2048 CHAR),
  payer_name               VARCHAR2(150 BYTE),
  age_18_59                CHAR(1 CHAR),
  age_60_85                CHAR(1 CHAR),
  diabetic                 CHAR(1 CHAR),
  diabetes_dx_code         VARCHAR2(100 BYTE),
  hypertension_dx_code     VARCHAR2(2048 BYTE),
  hypertension_onset_date  DATE,
  bp_reading_time          DATE,
  systolic_bp              NUMBER,
  diastolic_bp             NUMBER,
  numerator_flag1          CHAR(1 CHAR),
  numerator_flag2          CHAR(1 CHAR),
  numerator_flag3          CHAR(1 CHAR)
)
COMPRESS BASIC;

ALTER TABLE dsrip_report_tr018_qmed ADD CONSTRAINT pk_tr018_bp_report
PRIMARY KEY(network,patient_id,visit_id);
