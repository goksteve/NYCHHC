DROP TABLE dsrip_tr_024_amm_cdw CASCADE CONSTRAINTS;

CREATE TABLE dsrip_tr_024_amm_cdw
(
 network                 VARCHAR2(3 BYTE) NOT NULL,
 admission_dt_key        NUMBER(8) NOT NULL,
 visit_id                NUMBER(12) NOT NULL,
 visit_number            VARCHAR2(50 BYTE) NULL,
 facility_key            NUMBER(12) NOT NULL,
 visit_facility_id       NUMBER(2) NULL,
 visit_facility_name     VARCHAR2(64 BYTE) NULL,
 patient_id              NUMBER(12) NOT NULL,
 pat_lname               VARCHAR2(600 BYTE) NULL,
 mrn                     VARCHAR2(600 BYTE) NULL,
 birthdate               DATE NULL,
 age                     NUMBER NULL,
 apt_suite               VARCHAR2(1024 BYTE) NULL,
 street_address          VARCHAR2(1024 BYTE) NULL,
 city                    VARCHAR2(50 BYTE) NULL,
 state                   VARCHAR2(50 BYTE) NULL,
 country                 VARCHAR2(50 BYTE) NULL,
 mailing_code            VARCHAR2(50 BYTE) NULL,
 home_phone              VARCHAR2(50 BYTE) NULL,
 day_phone               VARCHAR2(50 BYTE) NULL,
 initial_visit_type_id   NUMBER(12) NULL,
 initial_visit_type      VARCHAR2(50 BYTE) NULL,
 visit_type_id           NUMBER(12) NULL,
 visit_type              VARCHAR2(50 BYTE) NULL,
 admission_dt            DATE NOT NULL,
 discharge_dt            DATE NULL,
 service                 VARCHAR2(4000 BYTE) NULL,
 medicaid_ind            CHAR(1 CHAR) NULL,
 payer_group             VARCHAR2(10 CHAR) NULL,
 payer_id                VARCHAR2(150 BYTE) NULL,
 payer_name              VARCHAR2(150 BYTE) NULL,
 pcp                     VARCHAR2(60 BYTE) NULL,
 plan_id                 NUMBER(12) NULL,
 plan_name               VARCHAR2(100 BYTE) NULL,
 icd_code                VARCHAR2(100 BYTE) NOT NULL,
 problem_comments        VARCHAR2(1024 BYTE) NULL,
 diagnosis_dt            DATE NOT NULL,
 drug_name               VARCHAR2(175 BYTE) NULL,
 drug_description        VARCHAR2(512 BYTE) NULL,
 dosage                  VARCHAR2(2048 BYTE) NULL,
 frequency               VARCHAR2(2048 BYTE) NULL,
 daily_pills_cnt         NUMBER(6) NULL,
rx_quantity             NUMBER(12) NULL,
 numerator_flag          NUMBER(2) NULL,
 order_dt                DATE NULL,
 next_order_dt           DATE NULL,
 second_next_order_dt    DATE NULL,
 third_next_order_dt     DATE NULL,

 dsrip_report            VARCHAR2(255 BYTE) DEFAULT 'TR024 Antidepressant Medication Management' NULL,
 report_dt               DATE DEFAULT TRUNC(SYSDATE, 'MONTH') NULL,
 load_dt                 DATE DEFAULT SYSDATE NULL
)
NOCOMPRESS
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'));

CREATE UNIQUE INDEX pk_dsrip_tr_024_amm_cdw
 ON dsrip_tr_024_amm_cdw(
  network,
  visit_id,
  patient_id,
  report_dt)
 LOGGING;

ALTER TABLE dsrip_tr_024_amm_cdw  ADD (
  CONSTRAINT pk_dsrip_tr_024_amm_cdw
  PRIMARY KEY
  (network, visit_id, patient_id,  report_dt)
  USING INDEX pk_dsrip_tr_024_amm_cdw
  ENABLE VALIDATE);

GRANT SELECT ON dsrip_tr_024_amm_cdw TO PUBLIC;