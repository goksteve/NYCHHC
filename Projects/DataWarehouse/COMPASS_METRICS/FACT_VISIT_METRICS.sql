BEGIN

 FOR r IN (
           SELECT
           object_type, object_name
           FROM
           user_objects
           WHERE
           object_name = 'FACT_VISIT_METRICS' AND object_type = 'TABLE'
          )
 LOOP
  EXECUTE IMMEDIATE 'DROP ' || r.object_type || ' ' || r.object_name;
 END LOOP;

END;
/

CREATE TABLE fact_visit_metrics
(
 network                     VARCHAR2(4 BYTE),
 visit_key                   NUMBER(12),
 visit_id                    NUMBER(12),
 patient_key                 NUMBER(18),
 admission_dt_key            NUMBER(8),
 visit_number                VARCHAR2(40 BYTE),
 facility_id                 NUMBER(12),
 facility                    VARCHAR2(100 BYTE),
 visit_type_id               NUMBER(12),
 visit_type                  VARCHAR2(254 BYTE),
 medicaid_ind                NUMBER(2),
 medicare_ind                NUMBER(2),
 patient_id                  VARCHAR2(1024 BYTE),
 mrn                         VARCHAR2(512 BYTE),
 patient_name                VARCHAR2(302 BYTE),
 sex                         VARCHAR2(8 BYTE),
 race                        VARCHAR2(100 BYTE),
 birthdate                   DATE,
 patient_age_at_admission    NUMBER(3),
 admission_dt                DATE,
 discharge_dt                DATE,
 asthma_ind                  NUMBER(3),
 bh_ind                      NUMBER(3),
 breast_cancer_ind           NUMBER(3),
 diabetes_ind                NUMBER(3),
 heart_failure_ind           NUMBER(3),
 hypertension_ind            NUMBER(3),
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
 nephropathy_screen_ind      NUMBER(3),
 retinal_dil_eye_exam_ind    NUMBER(3),
 tabacco_screen_proc_ind     NUMBER(3),
 a1c_final_calc_value        NUMBER(6, 2),
 gluc_final_calc_value       NUMBER(6, 2),
 ldl_final_calc_value        NUMBER(6, 2),
 bp_final_calc_value         VARCHAR2(324 BYTE),
 bp_final_calc_systolic      NUMBER(6),
 bp_final_calc_diastolic     NUMBER(6),
 source                      VARCHAR2(4 CHAR),
 load_dt                     DATE
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
/

CREATE UNIQUE INDEX idx_fact_visit_metrics1
 ON fact_visit_metrics(
  network,
  facility,
  visit_id,
  source)
 PARALLEL 32;

ALTER INDEX idx_fact_visit_metrics1
 NOPARALLEL;

GRANT SELECT ON fact_visit_metrics TO PUBLIC WITH GRANT OPTION;
/