EXEC dbm.drop_tables('DSRIP_TR017_DIAB_MON_CDW');

CREATE TABLE dsrip_tr017_diab_mon_cdw
(
 network                 CHAR(3 BYTE),
 admission_dt_key        NUMBER(8) NULL,
 comb_ind                NUMBER(2),
 gluc_ind                NUMBER(2),
 ldl_ind                 NUMBER(2),
 facility_id             NUMBER(12),
 facility_name           VARCHAR2(100 BYTE),
 patient_id              NUMBER(12),
 pat_lname               VARCHAR2(100 BYTE),
 pat_fname               VARCHAR2(100 BYTE),
 mrn                     VARCHAR2(512 BYTE),
 birthdate               DATE,
 age                     NUMBER(3),
 apt_suite               VARCHAR2(1024 BYTE) NULL,
 street_address          VARCHAR2(1024 BYTE) NULL,
 city                    VARCHAR2(50 BYTE) NULL,
 state                   VARCHAR2(50 BYTE) NULL,
 country                 VARCHAR2(50 BYTE) NULL,
 mailing_code            VARCHAR2(50 BYTE) NULL,
 home_phone              VARCHAR2(50 BYTE) NULL,
 day_phone               VARCHAR2(50 BYTE) NULL,
 pcp                     VARCHAR2(60 BYTE),
 visit_id                NUMBER(12),
 event_id                NUMBER(12),
 visit_type_id           NUMBER(12),
 visit_type              VARCHAR2(50 BYTE),
 admission_dt            DATE,
 discharge_dt            DATE,
 medicaid_ind            VARCHAR2(1 CHAR),
 payer_group             VARCHAR2(10 CHAR),
 payer_id                NUMBER(12),
 payer_name              CHAR(100 CHAR),
 plan_id                 NUMBER(12),
 plan_name               VARCHAR2(255 BYTE),
 icd_code                VARCHAR2(4000 BYTE),
 diagnose_desc           VARCHAR2(4000 BYTE),
 result_value            VARCHAR2(1023 BYTE),
 last_pcp_visit_date     DATE,
 last_bh_facility        VARCHAR2(255 BYTE),
 last_bh_visit_date      DATE,
 last_bh_provider_id     NUMBER(12),
 last_bh_provider        VARCHAR2(255),
 gluc_final_orig_value   VARCHAR2(1023 BYTE) NULL,
 gluc_final_calc_value   VARCHAR2(1023 BYTE) NULL,
 ldl_final_orig_value    VARCHAR2(1023 BYTE) NULL,
 ldl_final_calc_value    VARCHAR2(1023 BYTE) NULL,
 dsrip_report            VARCHAR2(55 BYTE) DEFAULT 'DSRIP_TR017_DIAB_MON' NULL,
 report_dt               DATE NULL,
 load_dt                 DATE DEFAULT SYSDATE
)
COMPRESS BASIC
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN') LOGGING COMPRESS BASIC,
  PARTITION gp1 VALUES ('GP1') LOGGING COMPRESS BASIC,
  PARTITION gp2 VALUES ('GP2') LOGGING COMPRESS BASIC,
  PARTITION nbn VALUES ('NBN') LOGGING COMPRESS BASIC,
  PARTITION nbx VALUES ('NBX') LOGGING COMPRESS BASIC,
  PARTITION qhn VALUES ('QHN') LOGGING COMPRESS BASIC,
  PARTITION sbn VALUES ('SBN') LOGGING COMPRESS BASIC,
  PARTITION smn VALUES ('SMN') LOGGING COMPRESS BASIC)
PARALLEL(DEGREE 8 INSTANCES 1)
MONITORING;

ALTER TABLE dsrip_tr017_diab_mon_cdw
 ADD CONSTRAINT pk_tr017_diab1_mon_cdw PRIMARY KEY(network, patient_id, report_dt);