ALTER TABLE fact_patient_diagnoses DROP PRIMARY KEY CASCADE;
DROP TABLE fact_patient_diagnoses CASCADE CONSTRAINTS;
CREATE TABLE fact_patient_diagnoses
(
 network                CHAR(3 BYTE) NOT NULL,
 patient_key            NUMBER(12) NOT NULL,
 problem_number         NUMBER(12) NOT NULL,
 diag_coding_scheme     VARCHAR2(50 BYTE) NULL,
 diag_code              VARCHAR2(100 BYTE) NOT NULL,
 problem_type           VARCHAR2(15 BYTE) NULL,
 problem_comments       VARCHAR2(1000 BYTE) NULL,
 provisional_flag       VARCHAR2(10 BYTE) NULL,
 onset_date             DATE NULL,
 end_date               DATE NULL,
 last_edit_time         DATE NULL,
 emp_provider_id        NUMBER(12) NULL,
 status_id              NUMBER(12) NULL,
 problem_status         VARCHAR2(60 BYTE) NULL,
 primary_problem        NUMBER(12) NULL,
 medical_problem_flag   VARCHAR2(3 BYTE) NULL,
 problem_list_type_id   NUMBER(12) NULL,
 problem_severity_id    NUMBER(12) NULL,
 load_date              DATE DEFAULT TRUNC(SYSDATE) NULL
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
  PARTITION smn VALUES ('SMN'))

PARALLEL(DEGREE 32 INSTANCES 1)
;

CREATE INDEX idx_fact_diags_pat
 ON fact_patient_diagnoses(network, patient_key)
 LOCAL (
  PARTITION cbn LOGGING NOCOMPRESS,
  PARTITION gp1 LOGGING NOCOMPRESS,
  PARTITION gp2 LOGGING NOCOMPRESS,
  PARTITION nbn LOGGING NOCOMPRESS,
  PARTITION nbx LOGGING NOCOMPRESS,
  PARTITION qhn LOGGING NOCOMPRESS,
  PARTITION sbn LOGGING NOCOMPRESS,
  PARTITION smn LOGGING NOCOMPRESS);

CREATE UNIQUE INDEX pk_fact_patient_diags
 ON fact_patient_diagnoses(
  patient_key,
  problem_number,
  diag_coding_scheme,
  diag_code)
 ;

ALTER TABLE fact_patient_diagnoses ADD (
  CONSTRAINT pk_fact_patient_diags
  PRIMARY KEY
  (patient_key, problem_number, diag_coding_scheme, diag_code)
  USING INDEX pk_fact_patient_diags
  ENABLE VALIDATE);

GRANT SELECT ON fact_patient_diagnoses TO PUBLIC;