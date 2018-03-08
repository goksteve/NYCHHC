CREATE TABLE problem_cmv_new
(
  network           CHAR(3 BYTE) NOT NULL,
  patient_id        NUMBER(12) NOT NULL,
  problem_number    NUMBER(12) NOT NULL,
  coding_scheme_id  VARCHAR2(50 BYTE) NOT NULL,
  code              VARCHAR2(100 BYTE) NOT NULL,
  description       VARCHAR2(2048 BYTE),
  CONSTRAINT pk_problem_cmv PRIMARY KEY(patient_id, problem_number, coding_scheme_id, code, network)
)
ORGANIZATION INDEX
PARTITION BY LIST (network)
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

GRANT SELECT ON problem_cmv_new TO PUBLIC;
