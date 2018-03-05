CREATE TABLE proc
(
  network            CHAR(3 BYTE) NOT NULL,
  proc_id            NUMBER(12) NOT NULL,
  name               VARCHAR2(175 BYTE),
  facility_id        NUMBER(12),
  kardex_group_id    NUMBER(12),
  proc_type_id       NUMBER(12),
  primary_proc_flag  VARCHAR2(2 BYTE),
  nursing_proc_flag  VARCHAR2(2 BYTE),
  used               CHAR(1 BYTE),
  cid                NUMBER(14),
  order_profile_id   NUMBER(12)
) COMPRESS BASIC
PARTITION BY LIST(network)
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

ALTER TABLE proc ADD CONSTRAINT pk_proc PRIMARY KEY(proc_id, network) USING INDEX LOCAL;

CREATE INDEX idx_proc_cid ON proc(cid);

GRANT SELECT ON proc TO PUBLIC;

CREATE OR REPLACE TRIGGER tr_insert_proc
FOR INSERT OR UPDATE ON proc
COMPOUND TRIGGER
  BEFORE STATEMENT IS
  BEGIN
    dwm.init_max_cids('PROC');
  END BEFORE STATEMENT;

  AFTER EACH ROW IS
  BEGIN
    dwm.max_cids(:new.network) := GREATEST(dwm.max_cids(:new.network), :new.cid);
  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    dwm.record_max_cids('PROC');
  END AFTER STATEMENT;
END tr_insert_proc;
/

