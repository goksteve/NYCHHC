EXEC dbm.drop_tables('DSRIP_TR022_DIAB_SCREEN_CDW');

CREATE TABLE dsrip_tr022_diab_screen_cdw
(
 network                        CHAR(3 BYTE) NOT NULL,
 admission_dt_key               NUMBER(8) NULL,
 facility_key                   NUMBER(12) NULL,
 facility_code                  CHAR(2 BYTE) NULL,
 facility_name                  VARCHAR2(64 BYTE) NULL,
 patient_id                     NUMBER(12) NOT NULL,
 pat_lname                      VARCHAR2(400 BYTE) NOT NULL,
 pat_fname                      VARCHAR2(400 BYTE) NOT NULL,
 mrn                            VARCHAR2(512 CHAR) NULL,
 birthdate                      DATE NOT NULL,
 age                            NUMBER(3) NOT NULL,
 apt_suite                      VARCHAR2(1024 BYTE) NULL,
 street_address                 VARCHAR2(1024 BYTE) NULL,
 city                           VARCHAR2(50 BYTE) NULL,
 state                          VARCHAR2(50 BYTE) NULL,
 country                        VARCHAR2(50 BYTE) NULL,
 mailing_code                   VARCHAR2(50 BYTE) NULL,
 home_phone                     VARCHAR2(50 BYTE) NULL,
 day_phone                      VARCHAR2(50 BYTE) NULL,
 pcp                            VARCHAR2(60 BYTE) NULL,
 visit_id                       NUMBER(12) NULL,
 visit_number                   VARCHAR2(40 BYTE) NULL,
 visit_type_id                  NUMBER(12) NULL,
 visit_type                     VARCHAR2(50 BYTE) NULL,
 admission_dt                   DATE NULL,
 discharge_dt                   DATE NULL,
 service_type                   VARCHAR2(50 BYTE) NULL,
 medicaid_ind                   CHAR(1 BYTE) NULL,
 payer_group                    VARCHAR2(10 BYTE) NULL,
 payer_key                      NUMBER(12) NULL,
 payer_name                     VARCHAR2(150 BYTE) NULL,
 plan_id                        NUMBER(12) NULL,
 plan_name                      VARCHAR2(100 BYTE) NULL,
 diabetes_flag                  NUMBER(2) NULL,
 diab_medication_flag           NUMBER(2) NULL,
 kidney_diag_num_flag           NUMBER(2) NULL,
 eye_exam_num_flag              NUMBER(2) NULL,
 eye_exam_latest_result_dt      DATE,
 eye_exam_result                VARCHAR2(24 BYTE),
 nephropathy_num_flag           NUMBER(2) NULL,
 nephropathy_latest_result_dt   DATE,
 hba1c_num_flag                 NUMBER(2) NULL,
 hba1c_latest_result            NUMBER(6, 2),
 hba1c_latest_result_dt         DATE,
 ace_arb_ind                    NUMBER(2) NULL,
 pcp_bh_flag                    VARCHAR2(48 BYTE),
 pcp_bh_service_dt              DATE,
 dsrip_report                   VARCHAR2(150 BYTE) DEFAULT 'DSRIP_TR022_COMPREHENSIVE_DIABETES SCREENING',
 report_dt                      DATE,
 source                         VARCHAR2(150 BYTE) NOT NULL,
 load_dt                        DATE DEFAULT TRUNC(SYSDATE)
)
COMPRESS BASIC
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'))
NOCACHE
MONITORING;

CREATE INDEX idx_dsrip_tr_022_diab_scrn_cdw
 ON dsrip_tr022_diab_screen_cdw(report_dt)
 LOCAL (
  PARTITION cbn,
  PARTITION gp1,
  PARTITION gp2,
  PARTITION nbn,
  PARTITION nbx,
  PARTITION qhn,
  PARTITION sbn,
  PARTITION smn);

CREATE UNIQUE INDEX pk_dsrip_tr_022_diab_scrn_cdw
 ON dsrip_tr022_diab_screen_cdw(
  network,
  patient_id,
  visit_id,
  report_dt)
 LOGGING;

ALTER TABLE dsrip_tr022_diab_screen_cdw ADD (
  CONSTRAINT pk_dsrip_tr_022_diab_scrn_cdw
  PRIMARY KEY
  (network, patient_id, visit_id, report_dt)
  USING INDEX pk_dsrip_tr_022_diab_scrn_cdw
  ENABLE VALIDATE);

GRANT SELECT ON dsrip_tr022_diab_screen_cdw TO PUBLIC;