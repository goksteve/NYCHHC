EXEC dbm.drop_tables('REF_PROC_DESCRIPTIONS');

CREATE TABLE ref_proc_descriptions
(
  proc_name   VARCHAR2(512),
  proc_type_id       NUMBER(6) NOT NULL,
  CONSTRAINT pk_ref_proc_descriptions PRIMARY KEY(proc_type_id, proc_name)
)
ORGANIZATION INDEX
PARTITION BY LIST (proc_type_id)
(
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
  PARTITION type_unknown VALUES (DEFAULT)
);

GRANT SELECT ON ref_drug_descriptions TO PUBLIC;
