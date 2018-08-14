DROP TABLE dsrip_tr024_025_cdw CASCADE CONSTRAINTS;

CREATE TABLE dsrip_tr024_025_cdw
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
 pat_fname               VARCHAR2(600 BYTE) NULL,
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
 pcp_bh_flag             VARCHAR2(100 BYTE) NULL,
 pcp_bh_service_dt       DATE,
 icd_code                VARCHAR2(100 BYTE) NOT NULL,
 problem_comments        VARCHAR2(1024 BYTE) NULL,
 diagnosis_dt            DATE NOT NULL,
 prescriber_id           NUMBER(12),
 prescriber_name         VARCHAR2(255 BYTE),
 prescriber_dept         VARCHAR2(1028 BYTE),
 drug_name               VARCHAR2(175 BYTE) NULL,
 drug_description        VARCHAR2(512 BYTE) NULL,
 dosage                  VARCHAR2(2048 BYTE) NULL,
 frequency               VARCHAR2(2048 BYTE) NULL,
 daily_pills_cnt         NUMBER(6) NULL,
 rx_quantity             NUMBER(12) NULL,
 order_dt                DATE NULL,
 tr_024_num_flag         NUMBER(2) NULL,
 tr_025_num_flag         NUMBER(2),
 next_order_dt           DATE NULL,
 second_next_order_dt    DATE NULL,
 third_next_order_dt     DATE NULL,
 fourth_next_order_dt    DATE,
 fifth_next_order_dt     DATE,
 six_next_order_dt       DATE,
 seven_next_order_dt     DATE,
 dsrip_report            VARCHAR2(500 BYTE) DEFAULT 'TR024_25 Antidepressant Medication Management' NULL,
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

CREATE UNIQUE INDEX pk_dsrip_tr_025_amm_cdw
 ON dsrip_tr024_025_cdw(
  network,
  visit_id,
  patient_id,
  report_dt)
 LOGGING;

ALTER TABLE dsrip_tr024_025_cdw  ADD (
  CONSTRAINT pk_dsrip_tr_025_amm_cdw
  PRIMARY KEY
  (network, visit_id, patient_id,  report_dt)
  USING INDEX pk_dsrip_tr_025_amm_cdw
  ENABLE VALIDATE);

GRANT SELECT ON dsrip_tr024_025_cdw TO PUBLIC;
CREATE OR REPLACE   PUBLIC SYNONYM dsrip_tr024_025_cdw FOR dsrip_tr024_025_cdw;