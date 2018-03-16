DROP TABLE dsrip_tr001_diagnoses PURGE;

CREATE TABLE dsrip_tr001_diagnoses
(
  network           VARCHAR2(3 BYTE),
  visit_id          NUMBER(12),
  coding_scheme_cd  VARCHAR2(6 BYTE),
  icd_cd            VARCHAR2(100 BYTE),
  is_primary        CHAR(1 BYTE), 
  CONSTRAINT pk_tst_tr001_diagnoses PRIMARY KEY(network, visit_id, coding_scheme_cd, icd_cd)
) ORGANIZATION INDEX;

GRANT SELECT ON dsrip_tr001_diagnoses TO PUBLIC;
 