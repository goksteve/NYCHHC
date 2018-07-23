EXEC dbm.drop_tables('fact_patient_prescrip_new');

CREATE TABLE FACT_PATIENT_PRESCRIP_NEW
(
 network            VARCHAR2(3 BYTE) NOT NULL,
 patient_key        NUMBER(18) NOT NULL,
 facility_key       NUMBER(12) NULL,
 order_dt_key       NUMBER(12) NOT NULL,
 patient_id         NUMBER(12) NOT NULL,
 mrn                VARCHAR2(512 BYTE),
 order_dt           DATE NOT NULL,
 drug_name          VARCHAR2(175 BYTE),
 drug_description   VARCHAR2(512 BYTE),
 rx_quantity        NUMBER(12),
 dosage             VARCHAR2(2048 BYTE),
 frequency          VARCHAR2(2048 BYTE),
 daily_cnt NUMBER(12),
 rx_refills NUMBER(12),
 load_dt            DATE DEFAULT TRUNC(SYSDATE)
)
COMPRESS BASIC
PARALLEL 32
PARTITION BY LIST (network)
 SUBPARTITION BY HASH (patient_id)
  SUBPARTITIONS 16
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'));



CREATE BITMAP INDEX bmi_patient_prescriptions
 ON fact_patient_prescrip_new(drug_description)
 PARALLEL 32
 LOCAL;

ALTER INDEX bmi_patient_prescriptions
 NOPARALLEL;

GRANT SELECT ON fact_patient_prescrip_new TO PUBLIC;

