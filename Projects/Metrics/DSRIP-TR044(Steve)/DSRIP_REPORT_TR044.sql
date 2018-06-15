DROP TABLE dsrip_report_tr044;

CREATE TABLE dsrip_report_tr044
(
  report_dt                 DATE,
  network                   CHAR(3 BYTE) NOT NULL,
  facility_name             VARCHAR2(100 BYTE),  
  patient_id                NUMBER(12) NOT NULL,
  name                      VARCHAR2(150 BYTE),
  birthdate                 DATE,
  mrn                       VARCHAR2(512 BYTE),
  visit_id                  NUMBER(12)   NOT NULL,
  visit_number              VARCHAR2(50 BYTE),
  home_phone                VARCHAR2(50 BYTE),
  day_phone                 VARCHAR2(50 BYTE),
  financial_class_name      VARCHAR2(100 BYTE),
  first_payer               VARCHAR2(150 BYTE),
  second_payer              VARCHAR2(150 BYTE),
  third_payer               VARCHAR2(150 BYTE),
  assigned_pcp              VARCHAR2(60 BYTE),
  pcp_visit_dt              DATE,
  pcp_visit_id              NUMBER(12),
  pcp_vst_provider          VARCHAR2(60 BYTE),
  pcp_vst_facility_name     VARCHAR2(64 BYTE),
  cardio_visit_dt           DATE,
  cardio_visit_id           NUMBER(12),
  cardio_vst_provider_name  VARCHAR2(60 BYTE),
  cardio_vst_facility_name  VARCHAR2(64 BYTE),
  mi_diagnosis_name         VARCHAR2(1000 BYTE),
  mi_onset_dt               DATE,
  ivd_diagnosis_name        VARCHAR2(1000 BYTE),
  ivd_onset_dt              DATE,
  statin_rx_dt              DATE,
  statin_rx_name            VARCHAR2(512 BYTE),
  statin_rx_quantity        NUMBER(12),
  numerator_flag            VARCHAR2(1 BYTE),
  pcp_flag                  VARCHAR2(1 BYTE),
  cardio_flag               VARCHAR2(1 BYTE),
  non_pcp_flag              VARCHAR2(1 BYTE),
  load_dt                   DATE DEFAULT SYSDATE NOT NULL,
  CONSTRAINT pk_tr044_report PRIMARY KEY(network, patient_id, visit_id)
);

GRANT SELECT ON dsrip_report_tr044 TO PUBLIC;