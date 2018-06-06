BEGIN

 FOR r
  IN (
      SELECT
      object_type, owner, object_name
      FROM
      all_objects
      WHERE
      owner = SYS_CONTEXT('userenv', 'current_schema')
      AND object_type IN ('TABLE')
      AND object_name IN ('STG_METRICS_DAILY_VISITS')
     )
 LOOP
  EXECUTE IMMEDIATE
   'drop ' || r.object_type || ' ' || r.owner || '.' || r.object_name || ' CASCADE CONSTRAINTS';
 END LOOP;

END;
/

CREATE TABLE STG_METRICS_DAILY_VISITS
(
 network         VARCHAR2(4 BYTE) NULL,
 visit_id        NUMBER(12) NOT NULL,
 visit_number    VARCHAR2(40 BYTE) NULL,
 facility_id     NUMBER(12) NULL,
 facility        VARCHAR2(100 BYTE) NULL,
 visit_type_id   NUMBER(12) NULL,
 visit_type      VARCHAR2(50 BYTE) NULL,
 medicaid_ind    NUMBER(2) NULL,
 medicare_ind    NUMBER(2) NULL,
 patient_id      NUMBER(12) NOT NULL,
 mrn             VARCHAR2(512 BYTE) NULL,
 pat_lname       VARCHAR2(150 BYTE) NULL,
 pat_fname       VARCHAR2(150 BYTE) NULL,
 sex             VARCHAR2(8 BYTE) NULL,
 birthdate       DATE NULL,
 age             NUMBER(3) NULL,
 admission_dt    DATE NULL,
 discharge_dt    DATE NULL,
 load_dt         DATE DEFAULT SYSDATE
)
LOGGING
COMPRESS BASIC
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'));

CREATE UNIQUE INDEX pk_stg_metrics_daily_visits
 ON stg_metrics_daily_visits(network, visit_id);

ALTER TABLE stg_metrics_daily_visits ADD (
  CONSTRAINT pk_stg_metrics_daily_visits
  PRIMARY KEY
  (network, visit_id)
  USING INDEX pk_stg_metrics_daily_visits
  ENABLE VALIDATE);

GRANT SELECT ON stg_metrics_daily_visits TO PUBLIC;