DROP TABLE DSRIP_TR018_BP_RESULTS;

CREATE TABLE dsrip_tr018_bp_results
(
  report_period_start_dt  DATE,
  report_period_end_dt    DATE,
  network                 VARCHAR2(3 BYTE),
  facility_id             NUMBER(12),
  clinic_code             VARCHAR2(4000 BYTE),
  clinic_code_service     VARCHAR2(4000 BYTE),
  clinic_code_desc        VARCHAR2(4000 BYTE),
  patient_id              NUMBER(12),
  visit_id                NUMBER,
  visit_type_name         VARCHAR2(50 BYTE),
  admission_date_time     DATE,
  discharge_date_time     DATE,
  financial_class_id      NUMBER,
  visit_financial_class   VARCHAR2(100 BYTE),
  payer_id                NUMBER,
  onset_date              DATE,
  htn_dx_code             VARCHAR2(100 BYTE)    NOT NULL,
  date_time               DATE,
  event_id                NUMBER(15),
  systolic_bp             NUMBER,
  diastolic_bp            NUMBER,
  flag_140_90             NUMBER,
  flag_150_90             NUMBER,
  rnum_per_patient        NUMBER
)
COMPRESS BASIC;



ALTER TABLE dsrip_tr018_bp_results ADD CONSTRAINT pk_tr018_bp_results1
PRIMARY KEY(network,patient_id,visit_id);
