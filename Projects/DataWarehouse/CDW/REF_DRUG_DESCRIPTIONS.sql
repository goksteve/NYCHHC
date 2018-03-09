EXEC dbm.drop_tables('REF_DRUG_DESCRIPTIONS');

CREATE TABLE ref_drug_descriptions
(
 drug_description   VARCHAR2(512),
 drug_type_id       NUMBER(6) NOT NULL,
 CONSTRAINT pk_ref_drug_descriptions PRIMARY KEY(drug_type_id, drug_description)
)
ORGANIZATION INDEX
PARTITION BY LIST (drug_type_id)
 (PARTITION type_5 VALUES (5),
  PARTITION type_22 VALUES (22),
  PARTITION type_24 VALUES (24),
  PARTITION type_28 VALUES (28),
  PARTITION type_33 VALUES (33),
  PARTITION type_34 VALUES (34),
  PARTITION type_40 VALUES (40),
  PARTITION type_41 VALUES (41),
  PARTITION type_42 VALUES (42),
  PARTITION type_43 VALUES (43),
  PARTITION type_44 VALUES (44),
  PARTITION type_45 VALUES (45),
  PARTITION type_46 VALUES (46),
  PARTITION type_unknown VALUES (DEFAULT));

GRANT SELECT ON ref_drug_descriptions TO PUBLIC;