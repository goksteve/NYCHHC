--DROP TABLE tr013_stg_asthma_ptnt_vsts;

CREATE TABLE tr013_stg_asthma_ptnt_vsts
(
  network                VARCHAR2(3 BYTE),
  report_run_dt          DATE,
  msrmnt_yr_end_dt       DATE,
  begin_dt               DATE,
  end_dt                 DATE,
  patient_id             NUMBER(12),
  visit_id               NUMBER(12),
  facility_id            NUMBER(12),
  facility_name          VARCHAR2(100 BYTE),
  latest_visit_type_id   NUMBER(12),
  latest_visit_type      VARCHAR2(50 BYTE),
  initial_visit_type_id  NUMBER(12),
  initial_visit_type     VARCHAR2(50 BYTE),
  admission_date_time    DATE,
  discharge_date_time    DATE,
  icd_code               VARCHAR2(100 BYTE),
  problem_description    VARCHAR2(1024 BYTE),
  payer_id               NUMBER(12),
  financial_class_id     NUMBER(12),
  fin_plan_name          VARCHAR2(100 BYTE),
  ptnt_prb_rnum          NUMBER,
  ptnt_vst_rnum          NUMBER
);

ALTER TABLE tr013_stg_asthma_ptnt_vsts ADD CONSTRAINT pk_stg_asthma_ptnt_visit PRIMARY KEY (network,patient_id,visit_id);