DROP TABLE fact_visit_monthly_metrics CASCADE CONSTRAINTS;

CREATE TABLE fact_visit_monthly_metrics
(
 network                     CHAR(3 BYTE) NULL,
 visit_key                   NUMBER(12) NOT NULL,
 visit_id                    NUMBER(12) NOT NULL,
 patient_key                 NUMBER(20) NOT NULL,
 admission_dt_key            NUMBER(8) NULL,
 visit_number                VARCHAR2(50 BYTE),
 facility_id                 NUMBER(12) NOT NULL,
 facility                    VARCHAR2(64 BYTE) NOT NULL,
 visit_type                  VARCHAR2(50 BYTE) NULL,
 medicaid_ind                NUMBER NULL,
 medicare_ind                NUMBER NULL,
 patient_id                  VARCHAR2(256 CHAR) NULL,
 mrn                         VARCHAR2(512 CHAR) NULL,
 patient_name                VARCHAR2(150 BYTE) NULL,
 sex                         VARCHAR2(8 BYTE) NULL,
 race                        VARCHAR2(100 BYTE) NULL,
 birthdate                   DATE NULL,
 patient_age_at_admission    NUMBER(3) NULL,
 admission_dt                DATE NULL,
 discharge_dt                DATE NULL,
 asthma_ind                  NUMBER(3) NULL,
 bh_ind                      NUMBER(3) NULL,
 breast_cancer_ind           NUMBER(12) NULL,
 diabetes_ind                NUMBER(3) NULL,
 heart_failure_ind           NUMBER(3) NULL,
 hypertension_ind            NUMBER(3) NULL,
 kidney_diseases_ind         NUMBER(3) NULL,
 smoker_ind                  NUMBER(3) NULL,
 pregnancy_ind               NUMBER(3) NULL,
 pregnancy_onset_dt          DATE NULL,
 flu_vaccine_ind             NUMBER(3) NULL,
 flu_vaccine_onset_dt        DATE,
 pna_vaccine_ind             NUMBER(3) NULL,
 pna_vaccine_onset_dt        DATE,
 bronchitis_ind              NUMBER(3) NULL,
 bronchitis_onset_dt         DATE,
 tabacco_scr_diag_ind        NUMBER(3) NULL,
 tabacco_scr_diag_onset_dt   DATE,
 nephropathy_screen_ind      NUMBER(3) NULL,
 retinal_dil_eye_exam_ind    NUMBER(3) NULL,
 tabacco_screen_proc_ind     NUMBER(3) NULL,
 a1c_final_calc_value        NUMBER(6, 2) NULL,
 gluc_final_calc_value       NUMBER(6, 2) NULL,
 ldl_final_calc_value        NUMBER(6, 2) NULL,
 bp_final_calc_value         VARCHAR2(255 BYTE) NULL,
 bp_final_calc_systolic      NUMBER(4) NULL,
 bp_final_calc_diastolic     NUMBER(4) NULL,
 source                      VARCHAR2(6) DEFAULT 'QCPR',
 load_dt                     DATE DEFAULT SYSDATE
)
NOLOGGING;

CREATE UNIQUE INDEX idx_fact_visit_monthly_metrics
 ON fact_visit_monthly_metrics(network, visit_id)
 LOGGING;

GRANT SELECT ON fact_visit_monthly_metrics TO PUBLIC;
CREATE OR REPLACE PUBLIC SYNONYM fact_visit_monthly_metrics FOR cdw.fact_visit_monthly_metrics;