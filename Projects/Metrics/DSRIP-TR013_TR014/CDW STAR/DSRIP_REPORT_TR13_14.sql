exec dbm.drop_tables('dsrip_report_tr13_14');

CREATE TABLE dsrip_report_tr13_14
(
  REPORT_RUN_DT                  DATE,
  BEGIN_DT                       DATE,
  END_DT                         DATE,
  NETWORK                        CHAR(3 BYTE) NOT NULL,
  FACILITY_NAME                  VARCHAR2(64 BYTE),
  PATIENT_ID                     NUMBER(12) NOT NULL,
  VISIT_ID                       NUMBER(12) NOT NULL,
  PATIENT_NAME                   VARCHAR2(100 BYTE),
  MRN                            VARCHAR2(512 BYTE),
  APT_SUITE                      VARCHAR2(1024 BYTE),
  STREET_ADDRESS                 VARCHAR2(1024 BYTE),
  CITY                           VARCHAR2(50 BYTE),
  STATE                          VARCHAR2(50 BYTE),
  COUNTRY                        VARCHAR2(50 BYTE),
  MAILING_CODE                   VARCHAR2(50 BYTE),
  HOME_PHONE                     VARCHAR2(50 BYTE),
  BIRTHDATE                      DATE,
  AGE                            NUMBER,
  LAST_PCP_VISIT_ID              NUMBER(12),
  LAST_PCP_VISIT_DT              DATE,
  FINAL_VISIT_TYPE               VARCHAR2(50 BYTE),
  INITIAL_VISIT_TYPE             VARCHAR2(50 BYTE),
  ADMISSION_DT                   DATE NOT NULL,
  DISCHARGE_DT                   DATE,
  PAYER_NAME                     VARCHAR2(150 BYTE),
  PAYER_GROUP                    VARCHAR2(2048 BYTE),
  PLAN_NAME                      VARCHAR2(100 BYTE),
  MEDICAID_IND                   CHAR(1 CHAR),
  ICD_CODE                       VARCHAR2(100 BYTE) NOT NULL,
  PROBLEM_DESCRIPTION            VARCHAR2(1024 BYTE),
  ASTHMA_ERLST_MED_NAME          VARCHAR2(175 BYTE),
  ROUTE                          VARCHAR2(512 BYTE),
  DOSAGE                         VARCHAR2(2048 BYTE),
  FREQUENCY                      VARCHAR2(2048 BYTE),
  ASTHMA_EARLST_RX_DT            DATE,
  SUM_DAYS_COVERED_IN_YR         NUMBER,
  TREATMENT_DAYS_IN_YEAR         NUMBER,
  PROPORTION_DAYS_COVERED        VARCHAR2(5 BYTE),
  numeraor_flag_75_med_ratio     CHAR(1 BYTE),
  numeraor_flag_50_med_ratio     CHAR(1 BYTE),
  CONSTRAINT pk_dsrip_report_tr13_14 PRIMARY KEY(report_run_dt, network, patient_id, visit_id) USING INDEX COMPRESS
)
COMPRESS BASIC;

GRANT SELECT ON dsrip_report_tr13_14 TO PUBLIC;