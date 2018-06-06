BEGIN
FOR r IN(
        SELECT  object_type,object_name
        FROM user_objects
        WHERE object_name = 'FACT_VISIT_METRICS' AND  object_type  = 'TABLE'
        )
LOOP
   EXECUTE immediate 'DROP '||r.object_type||' '||r.object_name ;
  END LOOP;
END;
/
CREATE TABLE FACT_VISIT_METRICS
(
 network                    VARCHAR2(4 BYTE),
 visit_id                   NUMBER(12),
 patient_key                NUMBER(12),
 admission_dt_key           NUMBER(8),
 visit_number               VARCHAR2(40 BYTE),
 facility                   VARCHAR2(100 BYTE),
 visit_type_id              NUMBER(12),
 visit_type                 VARCHAR2(254 BYTE),
 medicaid_ind               NUMBER(2),
 medicare_ind               NUMBER(2),
 patient_id                 VARCHAR2(1024 BYTE),
 mrn                        VARCHAR2(512 BYTE),
 patient_name               VARCHAR2(302 BYTE),
 sex                        VARCHAR2(8 BYTE),
 birthdate                  DATE,
 patient_age_at_admission   NUMBER(3),
 admission_dt               DATE,
 discharge_dt               DATE,
 asthma_ind                 NUMBER(2),
 bh_ind                     NUMBER(2),
 breast_cancer_ind          NUMBER(2),
 diabetes_ind               NUMBER(2),
 heart_failure_ind          NUMBER(2),
 hypertension_ind           NUMBER(2),
 kidney_diseases_ind        NUMBER(2),
 smoker_ind                 NUMBER(2),
 pregnancy_ind              NUMBER(2),
 pregnancy_onset_dt         DATE,
 nephropathy_screen_ind     NUMBER(2),
 retinal_dil_eye_exam_ind   NUMBER(2),
 a1c_final_calc_value       NUMBER(6),
 gluc_final_calc_value      NUMBER(6),
 ldl_final_calc_value       NUMBER(6),
 bp_final_calc_value        VARCHAR2(324 BYTE),
 bp_final_calc_systolic     NUMBER(6),
 bp_final_calc_diastolic    NUMBER(6),
 source                     VARCHAR2(4 CHAR),
 load_dt                    DATE
)
COMPRESS BASIC

PARTITION BY LIST (NETWORK)
(  
  PARTITION CBN VALUES ('CBN'),
  PARTITION GP1 VALUES ('GP1'),
  PARTITION GP2 VALUES ('GP2'),
  PARTITION NBN VALUES ('NBN'),
  PARTITION NBX VALUES ('NBX'),
  PARTITION QHN VALUES ('QHN'),
  PARTITION SBN VALUES ('SBN'),
  PARTITION SMN VALUES ('SMN')
);
/
CREATE UNIQUE INDEX idx_FACT_VISIT_METRICS
 ON FACT_VISIT_METRICS(network, facility, visit_id, source)
 PARALLEL 32;

ALTER INDEX idx_FACT_VISIT_METRICS
 NOPARALLEL;
GRANT SELECT ON FACT_VISIT_METRICS TO public WITH GRANT OPTION;
/