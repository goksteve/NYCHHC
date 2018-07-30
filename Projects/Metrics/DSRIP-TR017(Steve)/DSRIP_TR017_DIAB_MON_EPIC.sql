CREATE TABLE dsrip_tr_017_diab_mon_epic
(
 parent_location_name             VARCHAR2(255),
 location_id                      NUMBER(12),
 location_name                    VARCHAR2(80),
 mrn_empi                         VARCHAR2(100),
 location_mrn                     VARCHAR2(100),
 pat_name                         VARCHAR2(200),
 birth_date                       DATE,
 age_years                        NUMBER(3),
 address_line_1                   VARCHAR2(255),
 address_line_2                   VARCHAR2(255),
 address_state                    VARCHAR2(500),
 address_zip                      VARCHAR2(60),
 home_phone                       VARCHAR2(192),
 pcp_general_name                 VARCHAR2(200),
 contactdate                      DATE,
 hospital_discharge_date          DATE,
 encounter_type                   VARCHAR2(500),
 inspayor1                        VARCHAR2(500),
 inspayor2                        VARCHAR2(500),
 inspayor3                        VARCHAR2(500),
 icd10_codes                      VARCHAR2(510) NOT NULL,
 diabetic_medication_name         VARCHAR2(510),
 hemoglobin_order_time            DATE,
 hemoglobin_result_time           DATE,
 hemoglobin_result_value          VARCHAR2(500),
 ldl_c_order_time                 DATE,
 ldl_c__result_time               DATE,
 ldl_c_result_value               VARCHAR2(500),
 numr_flag_ldl_c_and_hemo_test    NUMBER(12),
 last_primary_care_visit_dt       DATE,
 last_primary_care_visit_dep_nm   VARCHAR2(500),
 last_behav_hlth_visit_date       DATE,
 last_behav_hlth_visit_dep_nm     VARCHAR2(500),
 last_behav_hlth_visit_prov_nm    VARCHAR2(200),
 source                           VARCHAR2(4),
 epic_flag                        VARCHAR2(1),
 report_dt                        DATE,
 etl_load_date                    DATE
);

CREATE UNIQUE INDEX idx_tr017_epic
 ON dsrip_tr_017_diab_mon_epic(
  mrn_empi,
  report_dt,
  hemoglobin_result_time,
  ldl_c__result_time)
 LOGGING;

GRANT SELECT ON dsrip_tr_017_diab_mon_epic TO PUBLIC;