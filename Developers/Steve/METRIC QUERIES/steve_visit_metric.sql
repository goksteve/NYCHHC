DROP TABLE steve_visit_metric CASCADE CONSTRAINTS;

CREATE TABLE steve_visit_metric
(
 network                    CHAR(3 BYTE) NOT NULL,
 visit_id                   NUMBER(12) NOT NULL,
 patient_key                NUMBER(12) NOT NULL,
 facility_key               NUMBER(12) NOT NULL,
 admission_dt_key           NUMBER(8) NOT NULL,
 discharge_dt_key           NUMBER(8) NULL,
 patient_id                 NUMBER(12) NOT NULL,
 admission_dt               DATE NOT NULL,
 discharge_dt               DATE NULL,
 patient_age_at_admission   NUMBER(3) NULL,
 a1c_final_result_date      DATE NULL,
 a1c_final_orig_value       VARCHAR2(4000 BYTE) NULL,
 a1c_final_calc_value       VARCHAR2(4000 BYTE) NULL,
 gluc_final_result_date     DATE NULL,
 gluc_final_orig_value      VARCHAR2(1023 BYTE) NULL,
 gluc_final_calc_value      VARCHAR2(4000 BYTE) NULL,
 ldl_final_result_date      DATE NULL,
 ldl_final_orig_value       VARCHAR2(1023 BYTE) NULL,
 ldl_final_calc_value       VARCHAR2(4000 BYTE) NULL,
 bp_final_result_date       DATE NULL,
 bp_final_orig_value        VARCHAR2(1023 BYTE) NULL,
 bp_calc_systolic           VARCHAR2(4000 BYTE) NULL,
 bp_calc_diastolic          VARCHAR2(12 BYTE) NULL
)
COMPRESS BASIC
NOLOGGING
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
  PARTITION smn VALUES ('SMN'))