set timing on

DROP TABLE tr013_stg_asthma_ptnt_vsts;

CREATE TABLE tr013_stg_asthma_ptnt_vsts
NOLOGGING
COMPRESS BASIC
AS
WITH 
--met criteria during both the measurement year and the year prior to the measurement year
  dt AS 
  (
    SELECT --+ materialize
      SUBSTR(ORA_DATABASE_NAME, 1, 3) network,
      TRUNC(SYSDATE, 'MONTH') AS report_run_dt,
      TRUNC(SYSDATE, 'YEAR') AS msrmnt_yr,
      ADD_MONTHS(TRUNC (SYSDATE ,'YEAR'),12)-1 AS msrmnt_yr_end_dt,
      ADD_MONTHS (TRUNC (SYSDATE, 'MONTH'), -24) begin_dt,
      TRUNC (SYSDATE, 'MONTH') end_dt
    FROM DUAL
  ),
--temp table to locate patient visits with a principal diagnosis of asthma with in the measurement period
  asthma_ptnt_lkp AS 
  (
    SELECT --+ materialize
      DISTINCT dt.network,
      dt.report_run_dt,
      dt.msrmnt_yr_end_dt,
      dt.begin_dt,
      dt.end_dt,
      vst_prb.patient_id,
      vst_prb.visit_id,
      vst.facility_id,
      f.name facility_name,
      vst.visit_type_id latest_visit_type_id,
      vst_type1.name latest_visit_type,
      vsg.visit_type_id initial_visit_type_id,
      vst_type2.name initial_visit_type,
      vst.admission_date_time,
      vst.discharge_date_time,
      vst_prb.code icd_code,
      vst_prb.description problem_description,
      vsp.payer_id,
      fc.financial_class_id,
      fc.name fin_plan_name,
      ROW_NUMBER() OVER (PARTITION BY vst_prb.patient_id ORDER BY vst.admission_date_time DESC) ptnt_prb_rnum,
      ROW_NUMBER() OVER (PARTITION BY network,vst_prb.patient_id,vst.visit_id ORDER BY vst.admission_date_time DESC) ptnt_vst_rnum
    FROM dt 
    JOIN ud_master.visit vst
      ON vst.admission_date_time BETWEEN dt.begin_dt AND dt.end_dt
    JOIN goreliks1.visit_event_icd_code vst_prb --TABLE WITH VISIT AND DIAGNOSIS RELATIONSHIP
      ON vst_prb.visit_id = vst.visit_id
    JOIN kollurug.meta_conditions metac -- ASTHMA DIAGNOSIS CODE METADATA
      ON metac.value = vst_prb.code 
     AND metac.criterion_id = 21
     AND INCLUDE_EXCLUDE_IND = 'I'
     AND metac.NETWORK='ALL'
    JOIN ud_master.visit_segment vsg
      ON vsg.visit_id=vst.visit_id 
     AND vsg.visit_segment_number=1 
    JOIN ud_master.facility f
      ON f.facility_id=vst.facility_id
    LEFT JOIN ud_master.visit_type vst_type1
     ON vst_type1.visit_type_id=vst.visit_type_id
    LEFT JOIN ud_master.visit_type vst_type2
     ON vst_type2.visit_type_id=vsg.visit_type_id
    LEFT JOIN ud_master.visit_segment_payer vsp
      ON vsp.visit_id = vst.visit_id
     AND vsp.visit_segment_number = 1
     AND vsp.payer_number = 1   
    LEFT JOIN ud_master.financial_class fc
      ON fc.financial_class_id=vst.financial_class_id
    WHERE               --and vst_prb.patient_id=545169
    NOT EXISTS
    (
      SELECT 
        cmv.patient_id
      FROM ud_master.problem_cmv cmv
      JOIN goreliks1.meta_conditions metac1
        ON metac1.value = cmv.code
      WHERE metac1.criterion_id = 21
       AND metac1.INCLUDE_EXCLUDE_IND = 'E'
       AND metac.NETWORK='ALL'
       AND cmv.patient_id = vst_prb.patient_id
    )  
  )
SELECT --+ parallel(4)
  a.network,
  a.report_run_dt,
  a.msrmnt_yr_end_dt,
  a.begin_dt,
  a.end_dt,  
  a.patient_id,
  a.visit_id,
  a.facility_id,
  a.facility_name,
  a.latest_visit_type_id,
  a.latest_visit_type,
  a.initial_visit_type_id,
  a.initial_visit_type,
  a.admission_date_time,
  a.discharge_date_time,
  a.icd_code,
  a.problem_description,
  a.payer_id,
  a.financial_class_id,
  a.fin_plan_name,
  a.ptnt_prb_rnum,
  a.ptnt_vst_rnum
FROM asthma_ptnt_lkp a
where ptnt_vst_rnum=1;


ALTER TABLE tr013_stg_asthma_ptnt_vsts ADD CONSTRAINT pk_stg_asthma_ptnt_visit PRIMARY KEY (network,patient_id,visit_id);