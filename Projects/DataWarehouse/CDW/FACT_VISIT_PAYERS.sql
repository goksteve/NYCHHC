ALTER TABLE fact_visit_payers DROP PRIMARY KEY CASCADE;

DROP TABLE fact_visit_payers CASCADE CONSTRAINTS;

CREATE TABLE fact_visit_payers
(
  network            CHAR(3 CHAR) NULL,
  visit_key          NUMBER(12) NOT NULL,
  visit_id           NUMBER(12) NOT NULL,
  first_payer_id     NUMBER NULL,
  first_payer_key    NUMBER NULL,
 second_payer_id    NUMBER NULL,
 second_payer_key   NUMBER NULL,
 third_payer_id     NUMBER NULL,
 third_payer_key    NUMBER NULL,
 fourth_payer_id    NUMBER NULL,
 fourth_payer_key   NUMBER NULL,
 load_dt date default trunc(sysdate)
)
COMPRESS BASIC
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN') ,
  PARTITION gp1 VALUES ('GP1') ,
  PARTITION gp2 VALUES ('GP2') ,
  PARTITION nbn VALUES ('NBN') ,
  PARTITION nbx VALUES ('NBX') ,
  PARTITION qhn VALUES ('QHN') ,
  PARTITION sbn VALUES ('SBN') ,
  PARTITION smn VALUES ('SMN') )
NOCACHE
MONITORING;

CREATE UNIQUE INDEX pk_fact_visit_payers  ON fact_visit_payers(visit_key) PARALLEL 32;
ALTER INDEX pk_fact_visit_payers NOPARALLEL;
ALTER TABLE fact_visit_payers ADD (  CONSTRAINT pk_fact_visit_payers  PRIMARY KEY  (visit_key)
  USING INDEX pk_fact_visit_payers  ENABLE VALIDATE);
GRANT SELECT ON fact_visit_payers TO PUBLIC;