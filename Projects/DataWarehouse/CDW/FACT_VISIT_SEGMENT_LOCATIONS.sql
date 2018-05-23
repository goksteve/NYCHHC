EXEC dbm.drop_tables('FACT_VISIT_SEGMENT_LOCATIONS');

CREATE TABLE fact_visit_segment_locations
(
 network                     CHAR(3 BYTE) NOT NULL,
 visit_id                    NUMBER(12) NOT NULL,
 visit_key                   NUMBER(12) NOT NULL,
 visit_segment_number        NUMBER(12) NOT NULL,
 location_id                 VARCHAR2(12 BYTE) NOT NULL,
 activation_time             DATE,
 visit_number                VARCHAR2(40 BYTE),
 visit_type_id               NUMBER(12),
 visit_subtype_id            NUMBER(12),
 financial_class_id          NUMBER(12),
 admitting_emp_provider_id   NUMBER(12),
 diagnosis                   VARCHAR2(200 BYTE),
 visit_service_type_id       NUMBER(12),
 facility_id                 NUMBER(12),
 arrival_mode_id             NUMBER(12),
 arrival_mode_string         VARCHAR2(100 BYTE),
 admit_event_id              NUMBER(12),
 last_edit_time              DATE,
 invalid_conversion_flag     VARCHAR2(7 BYTE),
 accident_on_job             VARCHAR2(5 BYTE),
 load_dt                     DATE DEFAULT TRUNC(SYSDATE) NOT NULL,
 cid                         NUMBER(14)
)
PARTITION BY LIST (network)
 SUBPARTITION BY HASH (visit_id)
  SUBPARTITIONS 16
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'));



CREATE UNIQUE INDEX pk_stg_visit_seg_loc
 ON fact_visit_segment_locations(
  network,
  visit_id,
  visit_segment_number,
  location_id)
 LOCAL
 PARALLEL 32;

ALTER INDEX pk_stg_visit_seg_loc
 NOPARALLEL;

ALTER TABLE fact_visit_segment_locations
 ADD CONSTRAINT pk_stg_visit_seg_loc PRIMARY KEY
      (network,
       visit_id,
       visit_segment_number,
       location_id)
      USING INDEX pk_stg_visit_seg_loc;

GRANT SELECT ON fact_visit_segment_locations TO PUBLIC;