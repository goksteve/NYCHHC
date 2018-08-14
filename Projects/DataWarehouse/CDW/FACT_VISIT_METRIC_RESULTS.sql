EXEC dbm.drop_tables('FACT_VISIT_METRIC_RESULTS');

CREATE TABLE fact_visit_metric_results
(
 network                       CHAR(3 BYTE) NULL,
 visit_id                      NUMBER(12) NOT NULL,
 visit_key                     NUMBER(12) NOT NULL,
 patient_key                   NUMBER(18) NOT NULL,
 facility_key                  NUMBER(12) NULL,
 admission_dt_key              NUMBER(8) NULL,
 discharge_dt_key              NUMBER(8) NULL,
 visit_number                  VARCHAR2(50 BYTE) NULL,
 patient_id                    NUMBER(12) NOT NULL,
 admission_dt                  DATE NULL,
 discharge_dt                  DATE NULL,
 patient_age_at_admission      NUMBER(3) NULL,
 first_payer_key               NUMBER(12) NULL,
 initial_visit_type_id         NUMBER(12) NULL,
 final_visit_type_id           NUMBER(12) NULL,
 asthma_ind                    NUMBER(3) NULL,
 bh_ind                        NUMBER(3) NULL,
 breast_cancer_ind             NUMBER(12) NULL,
 diabetes_ind                  NUMBER(3) NULL,
 heart_failure_ind             NUMBER(3) NULL,
 schizophrenia_ind             NUMBER(3) NULL,
 bipolar_ind                   NUMBER(3) NULL,
 hypertension_ind              NUMBER(3) NULL,
 kidney_diseases_ind           NUMBER(3) NULL,
 smoker_ind                    NUMBER(3) NULL,
 pregnancy_ind                 NUMBER(3) NULL,
 pregnancy_onset_dt            DATE NULL,
 flu_vaccine_ind               NUMBER(3) NULL,
 flu_vaccine_onset_dt          DATE NULL,
 pna_vaccine_ind               NUMBER(3) NULL,
 pna_vaccine_onset_dt          DATE NULL,
 bronchitis_ind                NUMBER(3) NULL,
 bronchitis_onset_dt           DATE NULL,
 tabacco_scr_diag_ind          NUMBER(3) NULL,
 tabacco_scr_diag_onset_dt     DATE NULL,
 major_depression_ind          NUMBER(3) NULL,
 retinal_dil_eye_exam_ind      NUMBER(3) NULL,
 retinal_final_result_dt       DATE NULL,
 retinal_eye_exam_value        VARCHAR2(20 BYTE) NULL,
 nephropathy_screen_ind        NUMBER(3) NULL,
 nephropathy_final_result_dt   DATE NULL,
 tabacco_screen_proc_ind       NUMBER(3) NULL,
 tabacco_final_result_dt       DATE NULL,
 a1c_final_result_dt           DATE NULL,
 a1c_final_calc_value          NUMBER(6, 2) NULL,
 gluc_final_result_dt          DATE NULL,
 gluc_final_calc_value         NUMBER(6, 2) NULL,
 ldl_final_result_dt           DATE NULL,
 ldl_final_calc_value          NUMBER(6, 2) NULL,
 bp_final_result_dt            DATE NULL,
 bp_final_calc_value           VARCHAR2(255 BYTE) NULL,
 bp_final_calc_systolic        NUMBER(4) NULL,
 bp_final_calc_diastolic       NUMBER(4) NULL,
 source                        VARCHAR2(6 CHAR) DEFAULT 'QCPR' NULL,
 load_dt                       DATE DEFAULT SYSDATE NULL
)
COMPRESS BASIC
PARALLEL 48
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

CREATE UNIQUE INDEX pk_fact_visit_metric_results
 ON fact_visit_metric_results(visit_id, network)
 LOCAL
 PARALLEL 32;

ALTER INDEX pk_fact_visit_metric_results
 NOPARALLEL;

ALTER TABLE fact_visit_metric_results
 ADD CONSTRAINT pk_fact_visit_metric_results PRIMARY KEY(visit_id, network)
     USING INDEX pk_fact_visit_metric_results;

GRANT SELECT ON fact_visit_metric_results TO PUBLIC;
CREATE OR REPLACE   PUBLIC SYNONYM fact_visit_metric_results FOR cdw.fact_visit_metric_results;
/