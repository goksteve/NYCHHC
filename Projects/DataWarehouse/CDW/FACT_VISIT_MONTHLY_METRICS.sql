DROP TABLE FACT_VISIT_MONTHLY_METRICS CASCADE CONSTRAINTS;

CREATE TABLE fact_visit_monthly_metrics
(
 network                    CHAR(3 BYTE) NULL,
 visit_id                   NUMBER(12) NOT NULL,
 admission_dt_key           NUMBER(8) NULL,
 facility                   VARCHAR2(64 BYTE) NOT NULL,
 visit_type                 VARCHAR2(50 BYTE) NULL,
 patient_id                 VARCHAR2(256 CHAR) NULL,
 mrn                        VARCHAR2(512 CHAR) NULL,
 patient_name               VARCHAR2(150 BYTE) NULL,
 sex                        VARCHAR2(8 BYTE) NULL,
 race                       VARCHAR2(100 BYTE) NULL,
 birthdate                  DATE NULL,
 patient_age_at_admission   NUMBER(3) NULL,
 admission_dt               DATE NULL,
 discharge_dt               DATE NULL,
 asthma_ind                 NUMBER(2) NULL,
 bh_ind                     NUMBER(2) NULL,
 breast_cancer_ind          NUMBER(12) NULL,
 diabetes_ind               NUMBER(2) NULL,
 heart_failure_ind          NUMBER(2) NULL,
 hypertension_ind           NUMBER(3) NULL,
 kidney_diseases_ind        NUMBER(2) NULL,
 smoker_ind                 NUMBER(3) NULL,
 pregnancy_ind              NUMBER(2) NULL,
 pregnancy_onset_dt         DATE NULL,
 nephropathy_screen_ind     NUMBER(2) NULL,
 retinal_dil_eye_exam_ind   NUMBER(2) NULL,
 a1c_final_calc_value       NUMBER(6, 2) NULL,
 gluc_final_calc_value      NUMBER(6, 2) NULL,
 ldl_final_calc_value       NUMBER(6, 2) NULL,
 bp_final_calc_value        VARCHAR2(255 BYTE) NULL,
 bp_final_calc_systolic     NUMBER(4) NULL,
 bp_final_calc_diastolic    NUMBER(4) NULL,
 medicaid_ind               NUMBER NULL,
 medicare_ind               NUMBER NULL
)
NOLOGGING
COMPRESS BASIC;

CREATE UNIQUE INDEX idx_fact_visit_monthly_metrics
 ON fact_visit_monthly_metrics(network, visit_id)
 LOGGING;

GRANT SELECT ON fact_visit_monthly_metrics TO PUBLIC;
CREATE OR REPLACE PUBLIC SYNONYM fact_visit_monthly_metrics FOR cdw.fact_visit_monthly_metrics;