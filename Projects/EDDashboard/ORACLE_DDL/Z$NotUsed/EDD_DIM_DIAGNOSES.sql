CREATE TABLE EDD_DIM_DIAGNOSES
(
  DIAGNOSISKEY  NUMBER(10) CONSTRAINT pk_edd_dim_diagnoses PRIMARY KEY,
  DIAGNOSIS     NVARCHAR2(1000)
) ORGANIZATION INDEX;

GRANT SELECT ON EDD_DIM_DIAGNOSES TO PUBLIC;