exec dbm.drop_tables('REF_VISIT_SERVICE_TYPES');

CREATE TABLE ref_visit_service_types
(
  network                CHAR(3 BYTE) NOT NULL,
  visit_type_id          NUMBER(12) NOT NULL,
  visit_service_type_id  NUMBER(12) NOT NULL,
  visti_service_type     VARCHAR2(100 BYTE),
  short_name             VARCHAR2(10 BYTE),
  facility_id            NUMBER(12),
  CONSTRAINT pk_ref_visit_service_types PRIMARY KEY(network, visit_type_id, visit_service_type_id)
) COMPRESS BASIC;

GRANT SELECT ON ref_visit_service_types TO PUBLIC;