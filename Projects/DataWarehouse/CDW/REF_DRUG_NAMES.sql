exec dbm.drop_tables('REF_DRUG_NAMES');

CREATE TABLE ref_drug_names
(
  drug_name     VARCHAR2(175),
  drug_type_id  NUMBER(6),
  CONSTRAINT pk_tst_drug_names PRIMARY KEY(drug_name, drug_type_id)
) ORGANIZATION INDEX
PARTITION BY LIST(drug_type_id)
(
  PARTITION type_5 VALUES(5),
  PARTITION type_22 VALUES(22),
  PARTITION type_24 VALUES(24),
  PARTITION type_28 VALUES(28),
  PARTITION type_33 VALUES(33),
  PARTITION type_34 VALUES(34),
  PARTITION type_35 VALUES(35),
  PARTITION type_na VALUES (DEFAULT)

);

GRANT SELECT ON ref_drug_names TO PUBLIC;
