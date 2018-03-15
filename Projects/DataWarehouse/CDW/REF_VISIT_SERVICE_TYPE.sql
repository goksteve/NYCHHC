EXEC dbm.drop_tables('REF_VISIT_SERVICE_TYPE');

CREATE TABLE ref_visit_service_type
(
 network                 CHAR(3 BYTE) NULL,
 facility_id             NUMBER(12) NULL,
 visit_type_id           NUMBER(12) NOT NULL,
 visit_service_type_id   NUMBER(12) NOT NULL,
 visti_service_type      VARCHAR2(100 BYTE) NULL,
 short_name              VARCHAR2(10 BYTE) NULL
)
COMPRESS BASIC;

CREATE UNIQUE INDEX pk_ref_visit_service_type
 ON ref_visit_service_type(network, visit_type_id, visit_service_type_id);

ALTER TABLE ref_visit_service_type ADD
(
  CONSTRAINT pk_ref_visit_service_type
  PRIMARY KEY
  (
network, visit_type_id, visit_service_type_id
)
  USING INDEX pk_ref_visit_service_type

);

CREATE INDEX idx_ref_visit_service_type  ON ref_visit_service_type
(
visit_service_type_id, network
);

GRANT SELECT ON ref_visit_types TO PUBLIC;
/