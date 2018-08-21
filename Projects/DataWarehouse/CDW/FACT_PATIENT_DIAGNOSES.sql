EXEC dbm.drop_tables('FACT_PATIENT_DIAGNOSES');

CREATE TABLE fact_patient_diagnoses
(
 network                   CHAR(3 BYTE) NOT NULL,
 patient_key               NUMBER(18) NOT NULL,
 patient_id                NUMBER(12) NOT NULL,
 problem_number            NUMBER(12) NOT NULL,
 diag_coding_scheme        VARCHAR2(50 BYTE) NULL,
 diag_code                 VARCHAR2(100 BYTE) NOT NULL,
 problem_type              VARCHAR2(15 BYTE) NULL,
 problem_comments          VARCHAR2(1000 BYTE) NULL,
 provisional_flag          VARCHAR2(10 BYTE) NULL,
 onset_date                DATE NULL,
 end_date                  DATE NULL,
 calc_diab_excl_end_date   DATE NULL,
 last_edit_time            DATE NULL,
 emp_provider_id           NUMBER(12) NULL,
 status_id                 NUMBER(12) NULL,
 problem_status            VARCHAR2(60 BYTE) NULL,
 primary_problem           NUMBER(12) NULL,
 medical_problem_flag      VARCHAR2(3 BYTE) NULL,
 problem_list_type_id      NUMBER(12) NULL,
 problem_severity_id       NUMBER(12) NULL,
 load_dt                   DATE DEFAULT TRUNC(SYSDATE) NULL
)
COMPRESS BASIC
PARALLEL 32
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'));

CREATE INDEX idx_fact_diags_pat_key
 ON fact_patient_diagnoses(network, patient_key)
 LOCAL
 PARALLEL 32;

ALTER INDEX idx_fact_diags_pat_key
 NOPARALLEL;

CREATE INDEX idx_fact_diags_pat_id
 ON fact_patient_diagnoses(network, patient_id)
 LOCAL
 PARALLEL 32;

ALTER INDEX idx_fact_diags_pat_id
 NOPARALLEL;

CREATE UNIQUE INDEX pk_fact_patient_diags
 ON fact_patient_diagnoses(
  patient_id,
  problem_number,
  diag_coding_scheme,
  diag_code,
  network)
 LOCAL
 PARALLEL 32;

ALTER INDEX pk_fact_patient_diags
 NOPARALLEL;

ALTER TABLE fact_patient_diagnoses
 ADD CONSTRAINT pk_fact_patient_diags PRIMARY KEY
      (patient_id,
       problem_number,
       diag_coding_scheme,
       diag_code,
       network)
      USING INDEX pk_fact_patient_diags;

GRANT SELECT ON fact_patient_diagnoses TO PUBLIC;