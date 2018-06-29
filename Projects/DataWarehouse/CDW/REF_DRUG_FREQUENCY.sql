DROP TABLE REF_DRUG_FREQUENCY CASCADE CONSTRAINTS;

CREATE TABLE ref_drug_frequency
(
 drug_frequency           VARCHAR2(512 BYTE) NULL,
 drug_frequency_num_val   NUMBER(6) NULL,
 med_route                VARCHAR2(30 CHAR) NULL
);

CREATE UNIQUE INDEX idx_ref_drug_frequancy
 ON ref_drug_frequency(drug_frequency);

CREATE  OR REPLACE PUBLIC SYNONYM ref_drug_frequency FOR ref_drug_frequency;
GRANT SELECT ON ref_drug_frequency TO PUBLIC WITH GRANT OPTION;

INSERT INTO
 ref_drug_frequency
 SELECT * FROM pt005.ganesh_ref_drug_frequency;

COMMIT;