exec dbm.drop_tables('FACT_VISIT_DIAGNOSES');
  
CREATE TABLE fact_visit_diagnoses
(
  network             CHAR(3 BYTE) NOT NULL,
  visit_id            NUMBER(12) NOT NULL,
  problem_nbr         NUMBER(12) NOT NULL,
  icd_code            VARCHAR2(100 BYTE) NOT NULL,
  coding_scheme       VARCHAR2(6 BYTE) NOT NULL,
  patient_key         NUMBER(12) NOT NULL,
  patient_id          NUMBER(12) NOT NULL,
  facility_key        NUMBER(12) NOT NULL,
  diagnosis_dt        DATE NOT NULL,
  diagnosis_dt_key    NUMBER(8) NOT NULL,
  is_primary_problem  CHAR(1 BYTE) NOT NULL,
  problem_comments    VARCHAR2(1024 BYTE),
  problem_status_id   NUMBER(12) NOT NULL,
  load_dt             DATE DEFAULT TRUNC(SYSDATE) NOT NULL,
  source              CHAR(4 BYTE) NOT NULL
) COMPRESS BASIC
PARTITION BY LIST (NETWORK)
SUBPARTITION BY HASH(visit_id) SUBPARTITIONS 16
(  
  PARTITION CBN VALUES ('CBN'),
  PARTITION GP1 VALUES ('GP1'),
  PARTITION GP2 VALUES ('GP2'),
  PARTITION NBN VALUES ('NBN'),
  PARTITION NBX VALUES ('NBX'),
  PARTITION QHN VALUES ('QHN'),
  PARTITION SBN VALUES ('SBN'),
  PARTITION SMN VALUES ('SMN')
);

GRANT SELECT ON FACT_VISIT_DIAGNOSES TO PUBLIC;

CREATE UNIQUE INDEX pk_fact_visit_diag ON fact_visit_diagnoses(icd_code, problem_nbr, network, visit_id) LOCAL PARALLEL 32;
ALTER INDEX pk_fact_visit_diag NOPARALLEL;

ALTER TABLE FACT_VISIT_DIAGNOSES ADD CONSTRAINT pk_fact_visit_diag  PRIMARY KEY(network, visit_id, problem_nbr, icd_code)
 USING INDEX pk_fact_visit_diag;
