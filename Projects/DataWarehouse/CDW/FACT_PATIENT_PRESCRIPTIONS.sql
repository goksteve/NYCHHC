EXEC dbm.drop_tables('FACT_PATIENT_PRESCRIPTIONS');

CREATE TABLE fact_patient_prescriptions
(
  network            VARCHAR2(3 BYTE) NOT NULL,
  facility_id        NUMBER(12),
  patient_id         NUMBER(12) NOT NULL,
  mrn                VARCHAR2(512 BYTE),
  order_dt           DATE NOT NULL,
  drug_name          VARCHAR2(175 BYTE),
  drug_description   VARCHAR2(512 BYTE),
  rx_quantity        NUMBER(12),
  dosage             VARCHAR2(2048 BYTE), 
  frequency          VARCHAR2(2048 BYTE),
  rx_dc_dt           DATE,
  rx_exp_dt          DATE,
  load_dt            DATE DEFAULT TRUNC(SYSDATE)
)
COMPRESS BASIC
PARTITION BY RANGE(order_dt) INTERVAL ( INTERVAL '1' YEAR )
SUBPARTITION BY LIST (network)
SUBPARTITION TEMPLATE
(
    SUBPARTITION cbn VALUES ('CBN'),
    SUBPARTITION gp1 VALUES ('GP1'),
    SUBPARTITION gp2 VALUES ('GP2'),
    SUBPARTITION nbn VALUES ('NBN'),
    SUBPARTITION nbx VALUES ('NBX'),
    SUBPARTITION qhn VALUES ('QHN'),
    SUBPARTITION sbn VALUES ('SBN'),
    SUBPARTITION smn VALUES ('SMN')
)
(
  PARTITION old_data VALUES LESS THAN (DATE '2010-01-01')
);

CREATE BITMAP INDEX bmi_drag_prescription_desc
ON fact_patient_prescriptions(drug_description)
LOCAL;

GRANT SELECT ON fact_patient_prescriptions TO PUBLIC;