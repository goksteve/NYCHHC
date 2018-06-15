EXEC dbm.drop_tables('FACT_VISIT_DIAGNOSES');

CREATE TABLE fact_visit_diagnoses
(
 network              CHAR(3 BYTE) NOT NULL,
 visit_id             NUMBER(12) NOT NULL,
 problem_nbr          NUMBER(12) NOT NULL,
 icd_code             VARCHAR2(100 BYTE) NOT NULL,
 coding_scheme        VARCHAR2(6 BYTE) NOT NULL,
 visit_key            NUMBER(12) NOT NULL,
 patient_key          NUMBER(18) NOT NULL,
 patient_id           NUMBER(18) NOT NULL,
 facility_key         NUMBER(12) NOT NULL,
 diagnosis_dt         DATE NOT NULL,
 diagnosis_dt_key     NUMBER(8) NOT NULL,
 is_primary_problem   CHAR(1 BYTE) NOT NULL,
 problem_comments     VARCHAR2(1024 BYTE),
 problem_status_id    NUMBER(12) NOT NULL,
 load_dt              DATE DEFAULT TRUNC(SYSDATE) NOT NULL,
 source               CHAR(4 BYTE) NOT NULL
)
COMPRESS BASIC
PARTITION BY LIST (network)
 SUBPARTITION BY HASH (visit_id)
  SUBPARTITIONS 16
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'));

GRANT SELECT ON fact_visit_diagnoses TO PUBLIC;

CREATE UNIQUE INDEX pk_fact_visit_diag
 ON fact_visit_diagnoses(
  icd_code,
  problem_nbr,
  network,
  visit_id)
 LOCAL
 PARALLEL 32;

ALTER INDEX pk_fact_visit_diag
 NOPARALLEL;

ALTER TABLE fact_visit_diagnoses
 ADD CONSTRAINT pk_fact_visit_diag PRIMARY KEY
      (network,
       visit_id,
       problem_nbr,
       icd_code)
      USING INDEX pk_fact_visit_diag;