rename problem to bkp_problem;

CREATE TABLE problem
(
  network               CHAR(3 BYTE) NOT NULL,
  patient_id            NUMBER(12) NOT NULL,
  problem_number        NUMBER(12) NOT NULL,
  problem_description   VARCHAR2(1000 BYTE),
  problem_type          VARCHAR2(15 BYTE),
  provisional_flag      VARCHAR2(10 BYTE),
  onset_date            DATE,
  start_date            DATE,
  stop_date             DATE,
  last_edit_time        DATE,
  emp_provider_id       NUMBER(12),
  status_id             NUMBER(12),
  primary_problem       NUMBER(12),
  medical_problem_flag  VARCHAR2(3 BYTE),
  problem_list_type_id  NUMBER,
  problem_severity_id   NUMBER
) COMPRESS BASIC
PARTITION BY LIST(network)
SUBPARTITION BY HASH(patient_id) SUBPARTITIONS 16 
(
  PARTITION cbn VALUES('CBN'),
  PARTITION gp1 VALUES('GP1'),
  PARTITION gp2 VALUES('GP2'),
  PARTITION nbn VALUES('NBN'),
  PARTITION nbx VALUES('NBX'),
  PARTITION qhn VALUES('QHN'),
  PARTITION sbn VALUES('SBN'),
  PARTITION smn VALUES('SMN')
);

CREATE UNIQUE INDEX pk_problem ON problem(problem_number, patient_id, network) LOCAL PARALLEL 32;
ALTER INDEX pk_problem NOPARALLEL;

ALTER TABLE problem ADD CONSTRAINT pk_problem PRIMARY KEY(network, patient_id, problem_number);

GRANT SELECT ON problem TO PUBLIC;


