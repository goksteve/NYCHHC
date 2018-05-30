DROP TABLE fact_daily_visits_stats_epic;

CREATE TABLE fact_daily_visits_stats_epic
(
  network                 CHAR(4 BYTE),
  facility_key            NUMBER,
  facility_name           VARCHAR2(100 BYTE),
  visit_id                NUMBER(18) NOT NULL,
  admission_dt            TIMESTAMP(6),
  discharge_dt            TIMESTAMP(6),
  visit_type              VARCHAR2(254 BYTE),
  patient_key             NUMBER,
  patient_id              VARCHAR2(18 BYTE),
  patient_name            VARCHAR2(200 BYTE),
  mrn                     VARCHAR2(408 BYTE),
  birth_date              TIMESTAMP(6),
  sex                     VARCHAR2(7 BYTE),
  age                     NUMBER,
  coding_scheme           CHAR(6 BYTE),
  diagnosis_name          VARCHAR2(200 BYTE),
  icd_code                VARCHAR2(254 BYTE),
  is_primary_problem      VARCHAR2(1 BYTE),
  asthma_ind              NUMBER,
  bh_ind                  NUMBER,
  breast_cancer_ind       NUMBER,
  diabetes_ind            NUMBER,
  heart_failure_ind       NUMBER,
  hypertansion_ind        NUMBER,
  kidney_diseases_ind     NUMBER,
  pregnancy_ind           NUMBER,
  pregnancy_onset_dt      TIMESTAMP(6),
  nephropathy_screen_ind  NUMBER,
  retinal_eye_exam_ind    NUMBER
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
  PARTITION smn VALUES ('SMN'));;

CREATE INDEX idx_fact_daily_vst_stats_epic ON fact_daily_visits_stats_epic (NETWORK, VISIT_ID);

GRANT SELECT on fact_daily_visits_stats_epic TO PUBLIC;
