EXEC dbm.drop_tables('REF_PROC_DESCRIPTIONS');

CREATE TABLE ref_proc_descriptions
(
 proc_key       NUMBER(12),
 network        VARCHAR2(6 BYTE),
 proc_id        NUMBER(12),
 proc_name      VARCHAR2(512) NOT NULL,
 proc_type_id   NUMBER(6) NOT NULL,
 in_ind         VARCHAR2(3 BYTE),
 source         VARCHAR2(6 BYTE) DEFAULT 'QCPR',
 load_dt        DATE DEFAULT SYSDATE,
 CONSTRAINT pk_ref_proc_descriptions PRIMARY KEY(proc_key, proc_type_id, in_ind)
)
ORGANIZATION INDEX
PARTITION BY LIST (proc_type_id)
  ( PARTITION type_2 VALUES (2),
  PARTITION type_12 VALUES (12),
  PARTITION type_14 VALUES (14),
  PARTITION type_16 VALUES (16),
  PARTITION type_87 VALUES (87),
  PARTITION type_88 VALUES (88),
  PARTITION type_89 VALUES (89),
  PARTITION type_90 VALUES (90),
  PARTITION type_91 VALUES (91),
  PARTITION type_92 VALUES (92),
  PARTITION type_93 VALUES (93),
  PARTITION type_94 VALUES (94),
  PARTITION type_95 VALUES (95),
  PARTITION type_96 VALUES (96),
  PARTITION type_97 VALUES (97),
  PARTITION type_98 VALUES (98),
  PARTITION type_unknown VALUES (DEFAULT));

GRANT SELECT ON ref_drug_descriptions TO PUBLIC;