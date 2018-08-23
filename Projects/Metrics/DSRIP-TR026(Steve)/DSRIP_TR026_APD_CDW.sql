DROP TABLE dsrip_tr026_apd_cdw CASCADE CONSTRAINTS;

CREATE TABLE DSRIP_TR026_APD_CDW
(
 network                  VARCHAR2(4 BYTE) NOT NULL,
 patient_id               NUMBER(12) NOT NULL,
 facility_id              NUMBER(12) NULL,
 facility_name            VARCHAR2(64 BYTE) NULL,
 pat_lname                VARCHAR2(600 BYTE) NULL,
 pat_fname                VARCHAR2(600 BYTE) NULL,
 mrn                      VARCHAR2(512 BYTE) NULL,
 birthdate                DATE NULL,
 age                      NUMBER(3) NULL,
 apt_suite                VARCHAR2(1024 BYTE) NULL,
 street_address           VARCHAR2(1024 BYTE) NULL,
 city                     VARCHAR2(50 BYTE) NULL,
 state                    VARCHAR2(50 BYTE) NULL,
 country                  VARCHAR2(50 BYTE) NULL,
 mailing_code             VARCHAR2(50 BYTE) NULL,
 home_phone               VARCHAR2(50 BYTE) NULL,
 day_phone                VARCHAR2(50 BYTE) NULL,
 visit_type_id            NUMBER(12) NULL,
 visit_type               VARCHAR2(50 BYTE) NULL,
 admission_dt             DATE NOT NULL,
 discharge_dt             DATE NULL,
 medicaid_ind             CHAR(1 BYTE) NULL,
 payer_group              VARCHAR2(10 BYTE) NULL,
 payer_id                 VARCHAR2(150 BYTE) NULL,
 payer_name               VARCHAR2(150 BYTE) NULL,
 plan_id                  NUMBER(12) NULL,
 plan_name                VARCHAR2(100 BYTE) NULL,
 earliest_prescribed_dt   DATE NOT NULL,
 drug_description         VARCHAR2(2048 BYTE) NULL,
 days_covered             NUMBER NULL,
 total_refils             NUMBER NULL,
 treatment_period         NUMBER NULL,
 pdc_ratio                NUMBER NULL,
 numerator_flag             NUMBER NULL,
 dsrip_report             VARCHAR2(55 BYTE) NULL,
 report_dt                DATE NULL,
 source                   VARCHAR2(55 BYTE) DEFAULT 'QCPR',
 load_dt                  DATE NULL
);

CREATE UNIQUE INDEX ui_dsrip_tr026_apd_cdw
 ON dsrip_tr026_apd_cdw(network, patient_id, report_dt);

CREATE BITMAP INDEX idx_dsrip_tr026_apd_cdw
 ON dsrip_tr026_apd_cdw(report_dt);

CREATE OR REPLACE PUBLIC SYNONYM dsrip_tr026_apd_cdw FOR dsrip_tr026_apd_cdw;
GRANT SELECT ON dsrip_tr026_apd_cdw TO PUBLIC;