CREATE TABLE visit_segment_visit_location
(
  network               CHAR(3 BYTE) NOT NULL,
  visit_id              NUMBER(12) NOT NULL,
  visit_segment_number  NUMBER(12) NOT NULL,
  location_id           VARCHAR2(12 BYTE) NOT NULL,
  used                  CHAR(1 BYTE),
  cid                   NUMBER(14)
)
COMPRESS BASIC
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

CREATE UNIQUE INDEX pk_visit_segm_location ON visit_segment_visit_location(visit_id, visit_segment_number, location_id, network) LOCAL COMPRESS PARALLEL 32;
ALTER INDEX pk_visit_segm_location NOPARALLEL;

ALTER TABLE visit_segment_visit_location ADD CONSTRAINT pk_visit_segm_location PRIMARY KEY(visit_id, visit_segment_number, location_id, network) 
 USING INDEX pk_visit_segm_location;

CREATE INDEX idx_visit_segm_location_cid ON visit_segment_visit_location(cid, network) LOCAL PARALLEL 32;
ALTER INDEX idx_visit_segm_location_cid NOPARALLEL;

GRANT SELECT ON visit_segment_visit_location TO PUBLIC;

CREATE OR REPLACE TRIGGER tr_insert_visit_segm_loc
FOR INSERT OR UPDATE ON visit_segment_visit_location
COMPOUND TRIGGER
  BEFORE STATEMENT IS
  BEGIN
    dwm.init_max_cids('VISIT_SEGMENT_VISIT_LOCATION');
  END BEFORE STATEMENT;

  AFTER EACH ROW IS
  BEGIN
    dwm.max_cids(:new.network) := GREATEST(dwm.max_cids(:new.network), :new.cid);
  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    dwm.record_max_cids('VISIT_SEGMENT_VISIT_LOCATION');
  END AFTER STATEMENT;
END tr_insert_visit_segm_loc;
/