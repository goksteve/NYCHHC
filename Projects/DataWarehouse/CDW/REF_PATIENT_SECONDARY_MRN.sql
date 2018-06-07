DROP TABLE REF_PATIENT_SECONDARY_MRN CASCADE CONSTRAINTS;

CREATE TABLE ref_patient_secondary_mrn
(
 network        CHAR(3 CHAR) NOT NULL,
 patient_id     NUMBER(12) NOT NULL,
 second_mrn    VARCHAR2(50 CHAR) NOT NULL,
 facility_key   NUMBER(12) NOT NULL,
 facility_id    NUMBER(12) NOT NULL
)
parallel 32
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'))

COMPRESS BASIC;

CREATE UNIQUE INDEX pk_second_mrn
 ON ref_patient_secondary_mrn (network,patient_id , second_mrn, facility_key)  PARALLEL 32  LOCAL;

ALTER INDEX pk_second_mrn  NOPARALLEL;

ALTER TABLE ref_patient_secondary_mrn
 ADD CONSTRAINT pk_second_mrn UNIQUE(network,patient_id , second_mrn, facility_key) USING INDEX pk_second_mrn;

GRANT SELECT ON ref_patient_secondary_mrn TO PUBLIC;