DROP TABLE fact_patient_prescrip_det CASCADE CONSTRAINTS;

CREATE TABLE fact_patient_prescrip_det
(
 network                 VARCHAR2(4 BYTE) NULL,
 patient_key             NUMBER(20) NULL,
 facility_id             NUMBER(18) NULL,
 facility_name           VARCHAR2(100 BYTE) NULL,
 datenum_order_dt_key    NUMBER(18) NULL,
 provider_id             NUMBER(18) NULL,
 patient_id              VARCHAR2(256 BYTE) NULL,
 order_visit_id          NUMBER(18) NULL,
 order_event_id          NUMBER(18) NULL,
 order_span_id           NUMBER(18) NULL,
 order_dt                DATE NULL,
 orig_drug_description   VARCHAR2(2048 BYTE) NULL,
 dosage                  VARCHAR2(2048 BYTE) NULL,
 frequency               VARCHAR2(2048 BYTE) NULL,
 rx_quantity             NUMBER(18) NULL,
 rx_refills              NUMBER(18) NULL,
 misc_name               VARCHAR2(2048 BYTE) NULL,
 prescription_status     VARCHAR2(2048 BYTE) NULL,
 prescription_type       VARCHAR2(2048 BYTE) NULL,
 proc_id                 NUMBER(18) NULL,
 drug_name               VARCHAR2(2048 BYTE) NULL,
 rx_allow_subs           VARCHAR2(50 BYTE) NULL,
 rx_comment              VARCHAR2(2048 BYTE) NULL,
 rx_dc_dt                DATE NULL,
 rx_dispo                VARCHAR2(128 BYTE) NULL,
 rx_exp_dt               DATE NULL,
 rx_id                   NUMBER(18) NULL,
 rx_number               VARCHAR2(15 BYTE) NULL,
 start_dt                DATE NULL,
 stop_dt                 DATE NULL,
 drug_description        VARCHAR2(2048 BYTE) NULL,
 source                  VARCHAR2(10 BYTE) NULL,
 load_dt                 DATE DEFAULT TRUNC(SYSDATE) NULL
)
PARTITION BY LIST (network)
 SUBPARTITION BY HASH (patient_id)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'),
  PARTITION epic VALUES ('EPIC'));

CREATE BITMAP INDEX bmi_patient_prescriptions_det
 ON fact_patient_prescrip_det(drug_description)
 LOCAL;

CREATE OR REPLACE PUBLIC SYNONYM fact_patient_prescrip_det FOR cdw.fact_patient_prescrip_det;
GRANT SELECT ON fact_patient_prescrip_det TO PUBLIC;