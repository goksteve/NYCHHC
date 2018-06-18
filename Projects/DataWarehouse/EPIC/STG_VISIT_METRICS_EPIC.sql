DROP TABLE stg_visit_metrics_epic;

CREATE TABLE stg_visit_metrics_epic
(
 network                    VARCHAR2(4 BYTE),
 facility_key               NUMBER,
 facility_name              VARCHAR2(100 BYTE),
 visit_id                   NUMBER(18) NOT NULL,
 admission_dt_key           NUMBER(8) NOT NULL,
 admission_dt               DATE,
 discharge_dt               DATE,
 visit_type                 VARCHAR2(254 BYTE),
 patient_key                NUMBER,
 patient_id                 VARCHAR2(18 BYTE),
 patient_name               VARCHAR2(200 BYTE),
 mrn                        VARCHAR2(408 BYTE),
 birthdate                  DATE,
 sex                        VARCHAR2(7 BYTE),
 patient_age_at_admission   NUMBER(3),
 -- coding_scheme              CHAR(6 BYTE),
 -- diagnosis_name             VARCHAR2(200 BYTE),
 -- icd_code                   VARCHAR2(254 BYTE),
 -- is_primary_problem         VARCHAR2(1 BYTE),
 asthma_ind                 NUMBER(3),
 bh_ind                     NUMBER(3),
 breast_cancer_ind          NUMBER(3),
 diabetes_ind               NUMBER(3),
 heart_failure_ind          NUMBER(3),
 hypertension_ind           NUMBER(3),
 kidney_diseases_ind        NUMBER(3),
 smoker_ind                 NUMBER(3),
 pregnancy_ind              NUMBER(3),
 pregnancy_onset_dt         DATE,
 flu_vaccine_ind            NUMBER(3),
 flu_vaccine_onset_dt       DATE,
 pna_vaccine_ind            NUMBER(3),
 pna_vaccine_onset_dt       DATE,
 nephropathy_screen_ind     NUMBER(3),
 retinal_eye_exam_ind       NUMBER(3),
 ldl_order_time             DATE,
 ldl_result_time            DATE,
 ldl_calc_value             NUMBER(5),
 bp_diastolic               NUMBER(5),
 bp_systolic                NUMBER(5),
 bp_orig_value              VARCHAR2(10 BYTE),
 bp_result_time             DATE,
 a1c_value                  NUMBER(5),
 a1c_result_dt              DATE,
 load_dt                    DATE DEFAULT SYSDATE
)
COMPRESS BASIC
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'));

--CREATE UNIQUE INDEX ui_fact_daili_visit_stats_epic
-- ON fact_daily_visits_stats_epic(
--  network,
--  facility_name,
--  visit_id,
--  diagnosis_name,
--  icd_code)
-- LOGGING;
--
--ALTER TABLE fact_daily_visits_stats_epic ADD (
--  CONSTRAINT ui_fact_daili_visit_stats_epic
--  UNIQUE (network, facility_name, visit_id, diagnosis_name, icd_code)
--  USING INDEX ui_fact_daili_visit_stats_epic
--  ENABLE VALIDATE);

CREATE INDEX idx_stg_visit_metrics_epic
 ON stg_visit_metrics_epic(network, visit_id);

GRANT SELECT ON stg_visit_metrics_epic TO PUBLIC;
