EXEC dbm.drop_tables('DSRIP_TR002_023_A1C_CDW');

CREATE TABLE dsrip_tr002_023_a1c_cdw
(
 network            CHAR(3 BYTE) NOT NULL,
 a1c_less_8         NUMBER(2) NULL,
 a1c_more_8         NUMBER(2) NULL,
 a1c_more_9         NUMBER(2) NULL,
 a1c_more_9_null    NUMBER(2) NULL,
 admission_dt_key   NUMBER(8) NULL,
 facility_key       NUMBER(12) NULL,
 facility_code      CHAR(2 BYTE) NULL,
 facility_name      VARCHAR2(64 BYTE) NULL,
 patient_id         NUMBER(12) NOT NULL,
 pat_lname          VARCHAR2(400 BYTE) NOT NULL,
 pat_fname          VARCHAR2(400 BYTE) NOT NULL,
 mrn                VARCHAR2(512 BYTE) NULL,
 birthdate          DATE NOT NULL,
 age                NUMBER NOT NULL,
 apt_suite          VARCHAR2(1024 BYTE) NULL,
 street_address     VARCHAR2(1024 BYTE) NULL,
 city               VARCHAR2(50 BYTE) NULL,
 state              VARCHAR2(50 BYTE) NULL,
 country            VARCHAR2(50 BYTE) NULL,
 mailing_code       VARCHAR2(50 BYTE) NULL,
 home_phone         VARCHAR2(50 BYTE) NULL,
 day_phone          VARCHAR2(50 BYTE) NULL,
 pcp                VARCHAR2(60 BYTE) NULL,
 visit_id           NUMBER NULL,
 visit_type_id      NUMBER NULL,
 visit_type         VARCHAR2(50 BYTE) NULL,
 admission_dt       DATE NULL,
 discharge_dt       DATE NULL,
 medicaid_ind       CHAR(1 CHAR) NULL,
 payer_group        VARCHAR2(10 CHAR) NULL,
 payer_key          NUMBER NULL,
 payer_name         VARCHAR2(150 BYTE) NULL,
 plan_id            NUMBER NULL,
 plan_name          VARCHAR2(100 BYTE) NULL,
 onset_date         DATE NULL,
 icd_code           VARCHAR2(100 BYTE) NOT NULL,
 problem_comments   VARCHAR2(1000 BYTE) NULL,
 latest_result_dt   DATE NULL,
 latest_result      NUMBER(6,2) NULL,
 dsrip_report       CHAR(24 BYTE) DEFAULT 'DSRIP_TR002_023' NULL,
 report_dt          DATE NULL,
 load_dt            DATE DEFAULT SYSDATE NULL
)
COMPRESS BASIC
PARALLEL 32
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'));

CREATE INDEX idx_dsrip_tr002_023
 ON dsrip_tr002_023_a1c_cdw(report_dt)
 LOCAL;

CREATE UNIQUE INDEX pk_dsrip_tr002_023_a1c_cdw
 ON dsrip_tr002_023_a1c_cdw(
  network,
  patient_id,
  visit_id,
  report_dt)
 LOGGING;

ALTER TABLE dsrip_tr002_023_a1c_cdw ADD (
  CONSTRAINT pk_dsrip_tr002_023_a1c_cdw
  PRIMARY KEY
  (network, patient_id, visit_id, report_dt)
  USING INDEX pk_dsrip_tr002_023_a1c_cdw);

GRANT SELECT ON dsrip_tr002_023_a1c_cdw TO PUBLIC;