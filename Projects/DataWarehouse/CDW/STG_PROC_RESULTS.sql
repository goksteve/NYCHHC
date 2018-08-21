drop table STG_PROC_RESULTS purge ;
CREATE TABLE STG_PROC_RESULTS
(
 network                         VARCHAR2(3 BYTE) NOT NULL,
 visit_id                        NUMBER(12) NOT NULL,
 event_id                        NUMBER(15) NOT NULL,
 result_report_number            NUMBER(12) NOT NULL,
 multi_field_occurrence_number   NUMBER(3) NOT NULL,
 item_number                     NUMBER(3) NOT NULL,
 result_dt                       DATE NOT NULL,
 visit_key                       NUMBER(12) not null,
 patient_key                     NUMBER(18) NOT NULL,
 patient_id                      NUMBER(12) NOT NULL,
 proc_facility_key               NUMBER(12) NOT NULL,
 proc_key                        NUMBER(12),
 modified_proc_name              VARCHAR2(2048 BYTE) NULL,
 event_status_id                 NUMBER(12),
 event_type_id                   NUMBER(12),
 data_element_id                 VARCHAR2(25 BYTE) NOT NULL,
 data_element_name               VARCHAR2(120 BYTE),
 result_value                    VARCHAR2(1023 BYTE),
 decode_source_id                VARCHAR2(40 BYTE),
 decoded_value                   VARCHAR2(1300 BYTE),
 load_dt                         DATE DEFAULT TRUNC(SYSDATE),
 cid                             NUMBER(14)
)
COMPRESS BASIC
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