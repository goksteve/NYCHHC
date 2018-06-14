DROP TABLE dim_patients CASCADE CONSTRAINTS;


CREATE TABLE dim_patients
(
  PATIENT_KEY                NUMBER(18) NOT NULL,
  NETWORK                    CHAR(3 BYTE) NOT NULL,
  PATIENT_ID                 NUMBER(12) NOT NULL,
  ARCHIVE_NUMBER             NUMBER(12) NOT NULL,
  NAME                       VARCHAR2(150 BYTE),
  PCP_PROVIDER_ID            NUMBER(12),
  PCP_PROVIDER_NAME          VARCHAR2(60 BYTE),
  TITLE_ID                   NUMBER(12),
  MEDICAL_RECORD_NUMBER      VARCHAR2(512 BYTE),
  SEX                        VARCHAR2(8 BYTE),
  BIRTHDATE                  DATE,
  DATE_OF_DEATH              DATE,
  APT_SUITE                  VARCHAR2(1024 BYTE),
  STREET_ADDRESS             VARCHAR2(1024 BYTE),
  CITY                       VARCHAR2(50 BYTE),
  STATE                      VARCHAR2(50 BYTE),
  COUNTRY                    VARCHAR2(50 BYTE),
  MAILING_CODE               VARCHAR2(50 BYTE),
  MARITAL_STATUS_ID          NUMBER(12),
  MARITAL_STATUS_DESC        VARCHAR2(100 BYTE),
  RACE_ID                    NUMBER(12),
  RACE_DESC                  VARCHAR2(100 BYTE),
  RELIGION_ID                NUMBER(12),
  RELIGION_DESC              VARCHAR2(100 BYTE),
  FREE_TEXT_RELIGION         VARCHAR2(100 BYTE),
  FREE_TEXT_OCCUPATION       VARCHAR2(100 BYTE),
  FREE_TEXT_EMPLOYER         VARCHAR2(150 BYTE),
  MOTHER_PATIENT_ID          NUMBER(12),
  COLLAPSED_INTO_PATIENT_ID  NUMBER(12),
  SOCIAL_SECURITY_NUMBER     VARCHAR2(15 BYTE),
  LIFECARE_VISIT_ID          NUMBER(12),
  CONFIDENTIAL_FLAG          VARCHAR2(1 BYTE),
  HOME_PHONE                 VARCHAR2(50 BYTE),
  DAY_PHONE                  VARCHAR2(50 BYTE),
  CELL_PHONE                 VARCHAR2(50 BYTE),  
  SMOKER_FLAG                VARCHAR2(1 BYTE),
  CURRENT_LOCATION           VARCHAR2(55 BYTE),
  SEC_LANG_NAME              VARCHAR2(100 BYTE),
  ADDR_STRING                VARCHAR2(256 BYTE),
  BLOCK_CODE                 VARCHAR2(256 BYTE),
  LAST_EDIT_TIME             DATE,
  COUNTY                     VARCHAR2(50 BYTE),
  SUB_BUILDING_NAME          VARCHAR2(256 BYTE),
  BUILDING_NAME              VARCHAR2(256 BYTE),
  BUILDING_NBR               VARCHAR2(256 BYTE),
  HEAD_OF_HOUSE_PATIENT_ID   NUMBER(12),
  CURRENT_FLAG               NUMBER(1) DEFAULT 1 NOT NULL CHECK(current_flag IN (0,1)),
  EFFECTIVE_FROM             DATE DEFAULT DATE '1901-01-01' NOT NULL,
  EFFECTIVE_TO               DATE DEFAULT DATE '9999-12-31' NOT NULL,
  SOURCE                     VARCHAR2(30 BYTE) DEFAULT 'QCPR',
  LOAD_DT                    DATE DEFAULT TRUNC(SYSDATE),
  LOADED_BY                  VARCHAR2(30 BYTE) DEFAULT SYS_CONTEXT('USERENV','OS_USER')
)
COMPRESS BASIC 
PARTITION BY LIST (NETWORK)
(  
  PARTITION CBN VALUES ('CBN'),
  PARTITION GP1 VALUES ('GP1'),
  PARTITION GP2 VALUES ('GP2'),
  PARTITION NBN VALUES ('NBN'),
  PARTITION NBX VALUES ('NBX'),
  PARTITION QHN VALUES ('QHN'),
  PARTITION SBN VALUES ('SBN'),
  PARTITION SMN VALUES ('SMN')
);

CREATE UNIQUE INDEX pk_dim_patients ON dim_patients(patient_key) PARALLEL 32;
ALTER INDEX pk_dim_patients NOPARALLEL;

ALTER TABLE dim_patients ADD CONSTRAINT pk_dim_patients PRIMARY KEY(patient_key) USING INDEX pk_dim_patients;

CREATE UNIQUE INDEX uk1_dim_patient ON dim_patients(patient_id, archive_number, network) PARALLEL 32 LOCAL;
ALTER INDEX uk1_dim_patient NOPARALLEL;

ALTER TABLE dim_patients ADD CONSTRAINT uk1_dim_patients UNIQUE(patient_id, archive_number, network) USING INDEX uk1_dim_patient;

CREATE UNIQUE INDEX uk2_dim_patient ON dim_patients
(
  CASE WHEN current_flag = 1 THEN patient_id END,
  CASE WHEN current_flag = 1 THEN network END
) PARALLEL 32;
ALTER INDEX uk2_dim_patient NOPARALLEL;

CREATE OR REPLACE PUBLIC SYNONYM dim_patients FOR dim_patients;

GRANT SELECT ON dim_patients TO PUBLIC WITH GRANT OPTION;
