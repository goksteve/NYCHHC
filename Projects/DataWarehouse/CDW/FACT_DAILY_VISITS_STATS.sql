--EXEC dbm.drop_tables('FACT_DAILY_VISITS_STATS');
DROP TABLE fact_daily_visits_stats CASCADE CONSTRAINTS;
CREATE TABLE FACT_DAILY_VISITS_STATS
(
 network         VARCHAR2(4 BYTE) NULL,
 visit_id        NUMBER(12) NOT NULL,
 visit_number    VARCHAR2(40 BYTE) NULL,
 facility_id     NUMBER (12) NULL,
 facility        VARCHAR2(100 BYTE) NULL,
 visit_type_id   NUMBER(12) NULL,
 visit_type      VARCHAR2(50 BYTE) NULL,
 medicaid_ind    NUMBER (2)NULL,
 medicare_ind    NUMBER (2) NULL,
 patient_id      NUMBER(12) NOT NULL,
 mrn             VARCHAR2(512 BYTE) NULL,
 pat_lname       VARCHAR2(150 BYTE) NULL,
 pat_fname       VARCHAR2(150 BYTE) NULL,
 sex             VARCHAR2(8 BYTE) NULL,
 birthdate       DATE NULL,
 age             NUMBER (3) NULL,
 admission_dt    DATE NULL,
 discharge_dt    DATE NULL,
 load_dt         DATE default sysdate
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

CREATE UNIQUE INDEX pk_fact_daily_visits_stats
 ON fact_daily_visits_stats(network, visit_id);

ALTER TABLE fact_daily_visits_stats ADD (
  CONSTRAINT pk_fact_daily_visits_stats
  PRIMARY KEY
  (network, visit_id)
  USING INDEX pk_fact_daily_visits_stats
  ENABLE VALIDATE);

GRANT SELECT ON fact_daily_visits_stats TO PUBLIC;