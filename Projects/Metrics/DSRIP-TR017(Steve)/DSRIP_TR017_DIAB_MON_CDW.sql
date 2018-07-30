ALTER TABLE dsrip_tr017_diab_mon_cdw  DROP PRIMARY KEY CASCADE;

DROP TABLE dsrip_tr017_diab_mon_cdw CASCADE CONSTRAINTS;

CREATE TABLE dsrip_tr017_diab_mon_cdw
(
 network                VARCHAR2(3 BYTE) NULL,
 admission_dt_key       NUMBER(8),
 comb_ind               NUMBER(1) NULL,
 a1c_ind                NUMBER(1) NULL,
 ldl_ind                NUMBER(1) NULL,
 facility_id            NUMBER(12) NULL,
 facility_name          VARCHAR2(100 BYTE) NULL,
 patient_id             NUMBER(12) NULL,
 pat_lname              VARCHAR2(100 BYTE) NULL,
 pat_fname              VARCHAR2(100 BYTE) NULL,
 mrn                    VARCHAR2(512 BYTE) NULL,
 birthdate              DATE NULL,
 age                    NUMBER(3) NULL,
 apt_suite              VARCHAR2(1024 BYTE) NULL,
 street_address         VARCHAR2(1024 BYTE) NULL,
 city                   VARCHAR2(50 BYTE) NULL,
 state                  VARCHAR2(50 BYTE) NULL,
 country                VARCHAR2(50 BYTE) NULL,
 mailing_code           VARCHAR2(50 BYTE) NULL,
 home_phone             VARCHAR2(50 BYTE) NULL,
 day_phone              VARCHAR2(50 BYTE) NULL,
 pcp                    VARCHAR2(60 BYTE) NULL,
 visit_id               NUMBER(12) NULL,
 visit_number           VARCHAR2(40 BYTE) NULL,
 visit_type_id          NUMBER(12) NULL,
 visit_type             VARCHAR2(50 BYTE) NULL,
 admission_dt           DATE NULL,
 discharge_dt           DATE NULL,
 medicaid_ind           VARCHAR2(1 BYTE) NULL,
 payer_group            VARCHAR2(10 BYTE) NULL,
 payer_id               NUMBER(12) NULL,
 payer_name             CHAR(100 BYTE) NULL,
 plan_id                NUMBER(12) NULL,
 plan_name              VARCHAR2(255 BYTE) NULL,
 test_type              VARCHAR2(12 BYTE) NULL,
 calc_result_value      VARCHAR2(1023 BYTE) NULL,
 last_pcp_facility      VARCHAR2(255 BYTE) NULL,
 last_pcp_visit_dt    DATE NULL,
 last_pcp_provider_id   NUMBER(12) NULL,
 last_pcp_provider      VARCHAR2(255 BYTE) NULL,
 last_bh_facility       VARCHAR2(255 BYTE) NULL,
 last_bh_visit_dt     DATE NULL,
 last_bh_provider_id    NUMBER(12) NULL,
 last_bh_provider       VARCHAR2(255 BYTE) NULL,
 dsrip_report           VARCHAR2(255 BYTE) NULL,
 report_dt              DATE NULL,
 load_dt                DATE DEFAULT SYSDATE NULL
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

CREATE UNIQUE INDEX pk_tr017_diab1_mon_cdw
 ON dsrip_tr017_diab_mon_cdw(
  network,
  patient_id,
  test_type,
  report_dt)
 LOGGING;

ALTER TABLE dsrip_tr017_diab_mon_cdw ADD (
  CONSTRAINT pk_tr017_diab1_mon_cdw
  PRIMARY KEY
  (network, patient_id, test_type, report_dt)
  USING INDEX pk_tr017_diab1_mon_cdw
  ENABLE VALIDATE);

GRANT SELECT ON dsrip_tr017_diab_mon_cdw TO PUBLIC;