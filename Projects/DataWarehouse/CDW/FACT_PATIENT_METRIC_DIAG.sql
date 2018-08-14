DROP TABLE fact_patient_metric_diag CASCADE CONSTRAINTS;

CREATE TABLE fact_patient_metric_diag
(
 patient_key                   NUMBER(20),
 network                       VARCHAR2(6 BYTE) NOT NULL,
 patient_id                    NUMBER(12) NOT NULL,
 asthma_ind                    NUMBER(3) NULL,
 asthma_f_onset_dt             DATE,
 asthma_l_onset_dt             DATE,
 asthma_l_end_dt               DATE,
 bh_ind                        NUMBER(3) NULL,
 bh_f_onset_dt                 DATE,
 bh_l_onset_dt                 DATE,
 bh_l_end_dt                   DATE,
 breast_cancer_ind             NUMBER(3) NULL,
 breast_cancer_f_onset_dt      DATE,
 breast_cancer_l_onset_dt      DATE,
 breast_cancer_l_end_dt        DATE,
 diabetes_ind                  NUMBER(3) NULL,
 diabetes_f_onset_dt           DATE,
 diabetes_l_onset_dt           DATE,
 diabetes_l_end_dt             DATE,
 heart_failure_ind             NUMBER(3) NULL,
 heart_failure_f_onset_dt      DATE,
 heart_failure_l_onset_dt      DATE,
 heart_failure_l_end_dt        DATE,
 schizophrenia_ind             NUMBER(3) NULL,
 schizophrenia_f_onset_dt      DATE,
 schizophrenia_l_onset_dt      DATE,
 schizophrenia_l_end_dt        DATE,
 bipolar_ind                   NUMBER(3) NULL,
 bipolar_f_onset_dt            DATE,
 bipolar_l_onset_dt            DATE,
 bipolar_l_end_dt              DATE,
 htn_ind                       NUMBER(3) NULL,
 htn_f_onset_dt                DATE,
 htn_l_onset_dt                DATE,
 htn_l_end_dt                  DATE,
 kidney_dz_ind                 NUMBER(3) NULL,
 kidney_dz_f_onset_dt          DATE,
 kidney_dz_l_onset_dt          DATE,
 kidney_dz_l_end_dt            DATE,
 smoker_ind                    NUMBER(3) NULL,
 smoker_f_onset_dt             DATE,
 smoker_l_onset_dt             DATE,
 smoker_l_end_dt               DATE,
 pregnancy_ind                 NUMBER(3) NULL,
 pregnancy_f_onset_dt          DATE,
 pregnancy_l_onset_dt          DATE,
 pregnancy_l_end_dt            DATE,
 flu_vaccine_ind               NUMBER(3) NULL,
 flu_vaccine_f_onset_dt        DATE,
 flu_vaccine_l_onset_dt        DATE,
 flu_vaccine_l_end_dt          DATE,
 pna_vaccine_ind               NUMBER(3) NULL,
 pna_vaccine_f_onset_dt        DATE,
 pna_vaccine_l_onset_dt        DATE,
 pna_vaccine_l_end_dt          DATE,
 bronchitis_ind                NUMBER(3) NULL,
 bronchitis_f_onset_dt         DATE,
 bronchitis_l_onset_dt         DATE,
 bronchitis_l_end_dt           DATE,
 tabacco_diag_ind              NUMBER(3) NULL,
 tabacco_diag_f_onset_dt       DATE,
 tabacco_diag_l_onset_dt       DATE,
 tabacco_diag_l_end_dt         DATE,
 major_depression_ind          NUMBER(3) NULL,
 major_depression_f_onset_dt   DATE,
 major_depression_l_onset_dt   DATE,
 major_depression_l_end_dt     DATE,
 load_dt                       DATE DEFAULT SYSDATE
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

CREATE UNIQUE INDEX pk_fact_patient_mertic_diag
 ON fact_patient_metric_diag(network, patient_id);

ALTER TABLE fact_patient_metric_diag ADD (
  CONSTRAINT pk_fact_patient_mertic_diag
  PRIMARY KEY
  (network, patient_id)
  USING INDEX pk_fact_patient_mertic_diag
  ENABLE VALIDATE);

GRANT SELECT ON fact_patient_metric_diag TO PUBLIC;

CREATE OR REPLACE PUBLIC SYNONYM fact_patient_metric_diag FOR cdw.fact_patient_metric_diag;