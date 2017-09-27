exec dbm.drop_tables('DIM_PATIENTS');

CREATE TABLE dim_Patients
(
  PatientKey NUMBER(10,0) CONSTRAINT pk_patients PRIMARY KEY,
  MRN NVARCHAR2(1000),
  PatientName NVARCHAR2(1000),
  Sex NVARCHAR2(1000),
  DOB NVARCHAR2(1000)
);

GRANT SELECT ON dim_Patients TO PUBLIC;