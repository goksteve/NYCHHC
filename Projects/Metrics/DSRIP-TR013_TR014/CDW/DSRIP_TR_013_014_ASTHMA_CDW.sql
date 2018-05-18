--DROP TABLE DSRIP_TR_013_014_ASTHMA_CDW CASCADE CONSTRAINTS;

CREATE TABLE dsrip_tr_013_014_asthma_cdw
(
 report_dt                    DATE NULL,
 begin_dt                     DATE NULL,
 end_dt                       DATE NULL,
 network                      CHAR(3 BYTE) NOT NULL,
 facility_name                VARCHAR2(64 BYTE) NULL,
 patient_id                   NUMBER(12) NOT NULL,
 visit_id                     NUMBER(12) NOT NULL,
 patient_name                 VARCHAR2(500 BYTE) NULL,
 mrn                          VARCHAR2(512 CHAR) NULL,
 apt_suite                    VARCHAR2(1024 BYTE) NULL,
 street_address               VARCHAR2(1024 BYTE) NULL,
 city                         VARCHAR2(50 BYTE) NULL,
 state                        VARCHAR2(50 BYTE) NULL,
 country                      VARCHAR2(50 BYTE) NULL,
 mailing_code                 VARCHAR2(50 BYTE) NULL,
 home_phone                   VARCHAR2(50 BYTE) NULL,
 birthdate                    DATE NULL,
 age                          NUMBER NULL,
 pcp_id                       NUMBER(12) NULL,
 pcp_name                     VARCHAR2(60 BYTE) NULL,
 last_pcp_visit_id            NUMBER(12) NULL,
 last_pcp_visit_dt            DATE NULL,
 final_visit_type             VARCHAR2(50 BYTE) NULL,
 initial_visit_type           VARCHAR2(50 BYTE) NULL,
 admission_dt                 DATE NOT NULL,
 discharge_dt                 DATE NULL,
 payer_name                   VARCHAR2(150 BYTE) NULL,
 payer_group                  VARCHAR2(2048 BYTE) NULL,
 plan_name                    VARCHAR2(100 BYTE) NULL,
 medicaid_ind                 CHAR(1 CHAR) NULL,
 icd_code                     VARCHAR2(100 BYTE) NOT NULL,
 problem_description          VARCHAR2(1024 BYTE) NULL,
 asthma_erlst_med_name        VARCHAR2(175 BYTE) NULL,
 route                        VARCHAR2(512 BYTE) NULL,
 dosage                       VARCHAR2(2048 BYTE) NULL,
 frequency                    VARCHAR2(2048 BYTE) NULL,
 asthma_earlst_rx_dt          DATE NULL,
 sum_days_covered_in_yr       NUMBER NULL,
 treatment_days_in_year       NUMBER NULL,
 proportion_days_covered      VARCHAR2(5 CHAR) NULL,
 numeraor_flag_75_med_ratio   CHAR(1 CHAR) NULL,
 numeraor_flag_50_med_ratio   CHAR(1 CHAR) NULL,
 load_dt                      DATE DEFAULT TRUNC(SYSDATE)
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
  PARTITION smn VALUES ('SMN'));

CREATE UNIQUE INDEX pk_dsrip_tr_013_014_asthma_cdw
 ON dsrip_tr_013_014_asthma_cdw(network, patient_id, report_dt)
 LOGGING;

GRANT SELECT ON dsrip_tr_013_014_asthma_cdw TO PUBLIC;