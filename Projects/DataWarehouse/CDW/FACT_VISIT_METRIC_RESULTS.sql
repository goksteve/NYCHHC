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
 a1c_final_result_date      DATE NULL,
 a1c_final_orig_value       VARCHAR2(1024 BYTE) NULL,
 a1c_final_calc_value       VARCHAR2(1024 BYTE) NULL,
 gluc_final_result_date     DATE NULL,
 gluc_final_orig_value      VARCHAR2(1024 BYTE) NULL,
 gluc_final_calc_value      VARCHAR2(1024 BYTE) NULL,
 ldl_final_result_date      DATE NULL,
 ldl_final_orig_value       VARCHAR2(1024 BYTE) NULL,
 ldl_final_calc_value       VARCHAR2(1024 BYTE) NULL,
 bp_final_result_date       DATE NULL,
 bp_final_orig_value        VARCHAR2(1024 BYTE) NULL,
 bp_calc_systolic           VARCHAR2(1024 BYTE) NULL,
 bp_calc_diastolic          VARCHAR2(12 BYTE) NULL,
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