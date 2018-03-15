exec dbm.drop_tables('VISIT_SECONDARY_NUMBER');

CREATE TABLE visit_secondary_number
(
  network CHAR(3 BYTE) NOT NULL,
  visit_id                NUMBER(12)            NOT NULL,
  visit_sec_nbr_type_id   NUMBER(12)            NOT NULL,
  visit_sec_nbr_nbr       NUMBER(12)            NOT NULL,
  visit_secondary_number  VARCHAR2(50 BYTE)
) COMPRESS BASIC
PARTITION BY LIST(network)
SUBPARTITION BY HASH(visit_id) SUBPARTITIONS 16
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

CREATE UNIQUE INDEX uk_visit_secondary_number ON visit_secondary_number
(
  CASE
    WHEN network = 'CBN' AND visit_sec_nbr_type_id IN (21,22)
      OR network = 'NBN' AND visit_sec_nbr_type_id = 9
      OR network IN ('NBX','QHN') AND visit_sec_nbr_type_id = 13
      OR network = 'SBN' AND visit_sec_nbr_type_id = 11
      OR network = 'SMN' AND visit_sec_nbr_type_id IN (12,15,17,18,24)
    THEN network
  END,
  CASE
    WHEN network = 'CBN' AND visit_sec_nbr_type_id IN (21,22)
      OR network = 'NBN' AND visit_sec_nbr_type_id = 9
      OR network IN ('NBX','QHN') AND visit_sec_nbr_type_id = 13
      OR network = 'SBN' AND visit_sec_nbr_type_id = 11
      OR network = 'SMN' AND visit_sec_nbr_type_id IN (12,15,17,18,24)
    THEN visit_id
  END,
  CASE
    WHEN network = 'CBN' AND visit_sec_nbr_type_id IN (21, 22)
      OR network = 'NBN' AND visit_sec_nbr_type_id = 9
      OR network IN ('NBX','QHN') AND visit_sec_nbr_type_id = 13
      OR network = 'SBN' AND visit_sec_nbr_type_id = 11
      OR network = 'SMN' AND visit_sec_nbr_type_id IN (12,15,17,18,24)
    THEN visit_sec_nbr_type_id
  END
) PARALLEL 32;
ALTER INDEX uk_visit_secondary_number NOPARALLEL;
   
CREATE UNIQUE INDEX pk_visit_secondary_number ON visit_secondary_number(visit_sec_nbr_type_id, visit_sec_nbr_nbr, visit_id, network) PARALLEL 32;
ALTER INDEX pk_visit_secondary_number NOPARALLEL;

ALTER TABLE visit_secondary_number ADD CONSTRAINT pk_visit_secondary_number PRIMARY KEY (network, visit_id, visit_sec_nbr_type_id, visit_sec_nbr_nbr)
 USING INDEX pk_visit_secondary_number;

GRANT SELECT ON visit_secondary_number TO PUBLIC;