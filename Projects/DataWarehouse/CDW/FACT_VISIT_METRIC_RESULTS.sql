EXEC dbm.drop_tables('FACT_VISIT_METRIC_RESULTS');

CREATE TABLE fact_visit_metric_results
(
 network                    CHAR(3 BYTE),
 visit_id                   NUMBER(12) NOT NULL,
 patient_key                NUMBER(12) NOT NULL,
 facility_key               NUMBER(12),
 admission_dt_key           NUMBER(8),
 discharge_dt_key           NUMBER(8),
 patient_id                 NUMBER(12) NOT NULL,
 admission_dt               DATE,
 discharge_dt               DATE,
 patient_age_at_admission   NUMBER(3),
 first_payer_key            NUMBER(12),
 initial_visit_type_id      NUMBER(12),
 final_visit_type_id        NUMBER(12),
 asthma_ind                 NUMBER(2) NULL,
 bh_ind                     NUMBER(2) NULL,
 breast_cancer_ind          NUMBER(12) NULL,
 diabetes_ind               NUMBER(2) NULL,
 heart_failure_ind          NUMBER(2) NULL,
 hypertansion_ind           NUMBER(2) NULL,
 kidney_diseases_ind        NUMBER(2) NULL,
 a1c_final_result_dt        DATE NULL,
 a1c_final_orig_value       VARCHAR2(1024 BYTE) NULL,
 a1c_final_calc_value       NUMBER(6,2) NULL,
 gluc_final_result_dt       DATE NULL,
 gluc_final_orig_value      VARCHAR2(1024 BYTE) NULL,
 gluc_final_calc_value      NUMBER(6,2) NULL,
 ldl_final_result_dt        DATE NULL,
 ldl_final_orig_value       VARCHAR2(1024 BYTE) NULL,
 ldl_final_calc_value       NUMBER(6,2) NULL,
 bp_final_result_dt         DATE NULL,
 bp_final_calc_value        VARCHAR2(255 BYTE) NULL,
 bp_final_calc_systolic     NUMBER(4) NULL,
 bp_final_calc_diastolic    NUMBER(4) NULL,
 load_dt                    DATE DEFAULT SYSDATE
)
COMPRESS BASIC
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