DROP TABLE fact_patient_metric_diag CASCADE CONSTRAINTS;

CREATE TABLE fact_patient_metric_diag1
(
 patient_key            NUMBER(20),
 network                CHAR(3 BYTE) NOT NULL,
 patient_id             NUMBER(12) NOT NULL,
 asthma_ind             NUMBER(3) NULL,
 bh_ind                 NUMBER(3) NULL,
 breast_cancer_ind      NUMBER(3) NULL,
 diabetes_ind           NUMBER(3) NULL,
 heart_failure_ind      NUMBER(3) NULL,
 hypertension_ind       NUMBER(3) NULL,
 kidney_diseases_ind    NUMBER(3) NULL,
 smoker_ind             NUMBER(3) NULL,
 pregnancy_ind          NUMBER(3) NULL,
 pregnancy_onset_dt     DATE NULL,
 flu_vaccine_ind        NUMBER(3) NULL,
 flu_vaccine_onset_dt   DATE NULL,
 pna_vaccine_ind        NUMBER(3) NULL,
 pna_vaccine_onset_dt   DATE NULL,
 bronchitis_ind         NUMBER(3) NULL,
 bronchitis_onset_dt    DATE NULL,
 load_dt                DATE DEFAULT SYSDATE
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

CREATE UNIQUE INDEX pk_fact_patient_mertic_diag
 ON fact_patient_metric_diag(network, patient_id);

ALTER TABLE fact_patient_metric_diag ADD (
  CONSTRAINT pk_fact_patient_mertic_diag
  PRIMARY KEY
  (network, patient_id)
  USING INDEX pk_fact_patient_mertic_diag
  ENABLE VALIDATE);

GRANT SELECT ON fact_patient_metric_diag TO PUBLIC;

CREATE OR REPLACE PUBLIC SYNONYM fact_patient_metric_diag FOR cdw.fact_patient_metric_diag;