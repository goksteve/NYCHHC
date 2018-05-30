DROP TABLE fact_patient_metric_diag CASCADE CONSTRAINTS;

CREATE TABLE fact_patient_metric_diag
(
 network               CHAR(3 BYTE) NOT NULL,
 patient_id            NUMBER(12) NOT NULL,
 asthma_ind            NUMBER(3) NULL,
 bh_ind                NUMBER(3) NULL,
 breast_cancer_ind     NUMBER(3) NULL,
 diabetes_ind          NUMBER(3) NULL,
 heart_failure_ind     NUMBER(3) NULL,
 hypertansion_ind      NUMBER(3) NULL,
 kidney_diseases_ind   NUMBER(3) NULL,
 smoker_ind            NUMBER(3) NULL,
 pregnancy_ind         NUMBER(3) NULL,
 pregnancy_onset_dt    DATE NULL
)
COMPRESS BASIC
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN') ,
  PARTITION nbx VALUES ('NBX') ,
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'))
;


CREATE UNIQUE INDEX PK_FACT_PATIENT_MERTIC_DIAG ON FACT_PATIENT_METRIC_DIAG(NETWORK, PATIENT_ID) ;

ALTER TABLE FACT_PATIENT_METRIC_DIAG ADD (
  CONSTRAINT PK_FACT_PATIENT_MERTIC_DIAG
  PRIMARY KEY
  (NETWORK, PATIENT_ID)
  USING INDEX PK_FACT_PATIENT_MERTIC_DIAG
  ENABLE VALIDATE);


GRANT SELECT ON fact_patient_metric_diag TO PUBLIC;