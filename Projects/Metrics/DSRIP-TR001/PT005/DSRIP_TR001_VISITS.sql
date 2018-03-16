DROP TABLE dsrip_tr001_visits PURGE;

CREATE TABLE dsrip_tr001_visits
(
  network                    VARCHAR2(3 BYTE),
  report_period_start_dt     DATE,
  facility_id                NUMBER(12),
  patient_id                 NUMBER(12),
  patient_name               VARCHAR2(100 BYTE),
  patient_dob                DATE,
  prim_care_provider         VARCHAR2(60 BYTE),
  visit_id                   NUMBER(12),
  visit_number               VARCHAR2(40 BYTE),
  mrn                        varchar2(30 BYTE),
  admission_dt               DATE,
  discharge_dt               DATE,
  visit_type_cd              CHAR(2 BYTE),
  fin_class                  VARCHAR2(100 BYTE),
  attending_emp_provider_id  NUMBER(12),
  resident_emp_provider_id   NUMBER(12),
  CONSTRAINT pk_tr001_visits PRIMARY KEY(network, visit_id)
) COMPRESS BASIC;

GRANT SELECT ON dsrip_tr001_visits TO PUBLIC;