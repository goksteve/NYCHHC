EXEC dbm.drop_tables('STG_PRESCRIPTION_DETAILS')


CREATE TABLE stg_prescription_details
(
 network            VARCHAR2(3 BYTE) NOT NULL,
 patient_id         NUMBER(12) NOT NULL,
 order_dt_key       NUMBER(8) NOT NULL,
 facility_ID       NUMBER(12)  NULL,
 order_dt           DATE NULL,
 drug_name          VARCHAR2(175 BYTE) NULL,
 drug_description   VARCHAR2(512 BYTE) NULL,
 rx_quantity        NUMBER(12) NULL,
 dosage             VARCHAR2(2048 BYTE) NULL,
 frequency          VARCHAR2(2048 BYTE) NULL,
 rx_dc_dt           DATE NULL,
 rx_exp_dt          DATE NULL
)
NOLOGGING
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

--CREATE INDEX idx_prescr_det
-- ON stg_prescription_details(network, patient_id)
-- PARALLEL 32;
--
--ALTER INDEX idx_prescr_det
-- NOPARALLEL;

GRANT SELECT ON stg_prescription_details TO PUBLIC;