CREATE TABLE visit_segment
(
  network                    CHAR(3 BYTE) NOT NULL,
  visit_id                   NUMBER(12) NOT NULL,
  visit_segment_number       NUMBER(12) NOT NULL,
  activation_time            DATE,
  visit_number               VARCHAR2(40 BYTE),
  visit_type_id              NUMBER(12),
  visit_subtype_id           NUMBER(12),
  financial_class_id         NUMBER(12),
  admitting_emp_provider_id  NUMBER(12),
  admitting_free_text        VARCHAR2(50 BYTE),
  diagnosis                  VARCHAR2(200 BYTE),
  visit_service_type_id      NUMBER(12),
  facility_id                NUMBER(12),
  visit_source_id            NUMBER(12),
  visit_source_string        VARCHAR2(100 BYTE),
  triage_acuity_id           NUMBER(12),
  arrival_mode_id            NUMBER(12),
  arrival_mode_string        VARCHAR2(100 BYTE),
  referral_type_id           NUMBER(12),
  referral_string            VARCHAR2(100 BYTE),
  admit_event_id             NUMBER(12),
  emp_provider_id            NUMBER(12),
  patient_type_id            VARCHAR2(10 BYTE),
  last_edit_time             DATE,
  comment_string             VARCHAR2(150 BYTE),
  prov_org_service_id        VARCHAR2(12 BYTE),
  sequence_id                NUMBER(12),
  invalid_conversion_flag    VARCHAR2(7 BYTE),
  referral_visit_id          NUMBER(12),
  referral_order_span_id     NUMBER(12),
  accident_code_id           NUMBER(12),
  accident_date_time         DATE,
  accident_description       VARCHAR2(250 BYTE),
  accident_details           VARCHAR2(250 BYTE),
  accident_on_job            VARCHAR2(5 BYTE),
  cid                        NUMBER(14)
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

CREATE UNIQUE INDEX pk_visit_segment ON visit_segment(visit_id, visit_segment_number, network) LOCAL COMPRESS PARALLEL 32;
ALTER INDEX pk_visit_segment NOPARALLEL;

ALTER TABLE visit_segment ADD CONSTRAINT pk_visit_segment PRIMARY KEY(visit_id, visit_segment_number, network) USING INDEX pk_visit_segment;

CREATE INDEX idx_visit_segment_cid ON visit_segment(cid, network) LOCAL PARALLEL 32;
ALTER INDEX idx_visit_segment_cid NOPARALLEL;

GRANT SELECT ON visit_segment TO PUBLIC;

CREATE OR REPLACE TRIGGER tr_insert_visit_segment
FOR INSERT OR UPDATE ON visit_segment
COMPOUND TRIGGER
  BEFORE STATEMENT IS
  BEGIN
    dwm.init_max_cids('VISIT_SEGMENT');
  END BEFORE STATEMENT;

  AFTER EACH ROW IS
  BEGIN
    dwm.max_cids(:new.network) := GREATEST(dwm.max_cids(:new.network), :new.cid);
  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    dwm.record_max_cids('VISIT_SEGMENT');
  END AFTER STATEMENT;
END tr_insert_visit_segment;
/
