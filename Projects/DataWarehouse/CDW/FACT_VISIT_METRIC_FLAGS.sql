DROP TABLE fact_visit_metric_flags CASCADE CONSTRAINTS PURGE;

CREATE TABLE fact_visit_metric_flags
(
 network                     CHAR(3 BYTE) NOT NULL,
 visit_id                    NUMBER(12) NOT NULL,
 visit_key                   NUMBER(12) NOT NULL,
 patient_key                 NUMBER(20) NOT NULL,
 facility_key                NUMBER(12),
 admission_dt_key            NUMBER(8),
 discharge_dt_key            NUMBER(8),
 visit_number                VARCHAR2(50 BYTE),
 patient_id                  NUMBER(12) NOT NULL,
 admission_dt                DATE,
 discharge_dt                DATE,
 patient_age_at_admission    NUMBER(3),
 first_payer_key             NUMBER(12),
 initial_visit_type_id       NUMBER(12),
 final_visit_type_id         NUMBER(12),
 asthma_ind                  NUMBER(3),
 bh_ind                      NUMBER(3),
 breast_cancer_ind           NUMBER(3),
 diabetes_ind                NUMBER(3),
 heart_failure_ind           NUMBER(3),
 schizophrenia_ind           NUMBER(3),
 bipolar_ind                 NUMBER(3),
 hypertansion_ind            NUMBER(3),
 kidney_diseases_ind         NUMBER(3),
 smoker_ind                  NUMBER(3),
 pregnancy_ind               NUMBER(3),
 pregnancy_onset_dt          DATE,
 flu_vaccine_ind             NUMBER(3),
 flu_vaccine_onset_dt        DATE,
 pna_vaccine_ind             NUMBER(3),
 pna_vaccine_onset_dt        DATE,
 bronchitis_ind              NUMBER(3),
 bronchitis_onset_dt         DATE,
 tabacco_scr_diag_ind        NUMBER(3),
 tabacco_scr_diag_onset_dt   DATE,
 major_depression_ind        NUMBER(3),
 nephropathy_screen_ind      NUMBER(3),
 retinal_dil_eye_exam_ind    NUMBER(3),
 retinal_eye_exam_rslt       VARCHAR2(20 BYTE),
 tabacco_screen_proc_ind     NUMBER(3),
 a1c_final_result_dt         DATE,
 a1c_final_orig_value        VARCHAR2(1023 BYTE),
 a1c_flag                    NUMBER(3),
 gluc_final_result_dt        DATE,
 gluc_final_orig_value       VARCHAR2(1023 BYTE),
 gluc_flag                   NUMBER,
 ldl_final_result_dt         DATE,
 ldl_final_orig_value        VARCHAR2(1023 BYTE),
 ldl_flag                    NUMBER,
 bp_final_result_dt          DATE,
 bp_final_orig_value         VARCHAR2(1023 BYTE),
 bp_flag                     NUMBER(3),
 source                      VARCHAR2(6 BYTE) DEFAULT 'QCPR',
 load_dt                     DATE DEFAULT SYSDATE
)
PARTITION BY LIST (network)
 SUBPARTITION BY HASH (visit_id)
  SUBPARTITIONS 16
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'));

CREATE UNIQUE INDEX IDX_FACT_VISIT_METRIC_FLAGS ON FACT_VISIT_METRIC_FLAGS
(NETWORK, VISIT_ID, PATIENT_ID)
LOGGING;

GRANT SELECT ON fact_visit_metric_flags TO PUBLIC;
CREATE OR REPLACE   PUBLIC SYNONYM fact_visit_metric_flags FOR cdw.fact_visit_metric_flags;