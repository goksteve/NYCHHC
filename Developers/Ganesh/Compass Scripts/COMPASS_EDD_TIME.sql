DROP TABLE COMPASS_EDD_TIME;
CREATE TABLE COMPASS_EDD_TIME
(
  REPORT_MNTH          DATE,
  METRIC_NAME        VARCHAR2(200 BYTE)         NOT NULL,
  DISPOSITION_CLASS  VARCHAR2(20 BYTE),
  ESI_KEY            NUMBER(10),
  BHC                VARCHAR2(7 BYTE),
  CIH                VARCHAR2(7 BYTE),
  HLM                VARCHAR2(7 BYTE),
  JMC                VARCHAR2(7 BYTE),
  KCH                VARCHAR2(7 BYTE),
  LHC                VARCHAR2(7 BYTE),
  MHC                VARCHAR2(7 BYTE),
  NCB                VARCHAR2(7 BYTE),
  WHH                VARCHAR2(7 BYTE),
  ELM                VARCHAR2(7 BYTE),
  QHC                VARCHAR2(7 BYTE),
  ALL_FACILITIES     VARCHAR2(7 BYTE)
);

CREATE OR REPLACE VIEW V_COMPASS_EDD_TIME
AS
select report_mnth,  metric_name, disposition_class, esi_key, bhc, cih, hlm, jmc, kch, lhc, mhc, ncb, whh, elm, qhc, ALL_FACILITIES
from
(
  with dt
  as
  (
    SELECT --+ materialize
      NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) AS report_mnth
    FROM dual
  )
  select
    dt.report_mnth, m.description metric_name, v.disposition_class, v.esi_key, 
    f.FacilityCode facility_code,
    to_char(trunc(nvl(v.metric_value,0)/60), '99')||':'||ltrim(to_char(mod(nvl(v.metric_value,0),60),'09')) metric_value
  from dt 
  join edd_fact_metric_values v on v.month_dt = dt.report_mnth 
  join edd_meta_metrics m on m.metric_id = v.metric_id AND v.disposition_class = 'ANY'
  left join edd_dim_facilities f on f.FacilityKey = v.facility_key
  )
  pivot
(
  max(metric_value)
  for Facility_Code in 
  (
    'BHC' as bhc,
    'CIH' as cih,
    'ELM' as elm,
    'HLM' as hlm,
    'JMC' as jmc,
    'KCHC' as kch,
    'LHC' as lhc,
    'MHC' as mhc,
    'NCB' as ncb,
    'QHC' as qhc,
    'WHH' as whh,
    'ALL' as ALL_FACILITIES
  )
)
order by esi_key, metric_name;
  

CREATE OR REPLACE PROCEDURE SP_REFRESH_COMPASS_EDD_TIME(p_report_month IN DATE DEFAULT NULL)
IS  
   d_report_mon   DATE;
   n_cnt          PLS_INTEGER := 0;
BEGIN
  xl.open_log('PREPARE_EDD_THROUGHPUT_DATA', SYS_CONTEXT('USERENV','OS_USER')||': Generating EDD throughput metrics data', TRUE);

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DDL';
  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

  xl.begin_action('Setting the report month');
  d_report_mon := TRUNC(NVL(p_report_month, SYSDATE), 'MONTH');
  dbms_session.set_identifier(d_report_mon);
  xl.end_action('Set to '||d_report_mon);

  xl.begin_action('Deleting old EDD throughput data (if any) for '||d_report_mon);  
      
  DELETE FROM COMPASS_EDD_TIME WHERE report_mnth = d_report_mon;
  n_cnt := n_cnt + SQL%ROWCOUNT;
  
  COMMIT;
  xl.end_action(n_cnt||' rows deleted');  

  etl.add_data 
  (
    i_operation   => 'INSERT /*+ parallel(32) append */',
    i_tgt         => 'COMPASS_EDD_TIME',
    i_src         => 'V_COMPASS_EDD_TIME',
    i_commit_at   => -1
  );

  
xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;
/



DECLARE
   v_report_mon   DATE;
   n_cnt          PLS_INTEGER := 0;
BEGIN
--    xl.open_log('Loading EDD volume data from 2017', SYS_CONTEXT('USERENV','OS_USER')||': Generating EDD volume data from Jan-2017', TRUE);

    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DDL';
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
   FOR i IN 0 .. 16
LOOP
      v_report_mon := ADD_MONTHS (DATE '2017-01-01', i);
      SP_REFRESH_COMPASS_EDD_TIME(v_report_mon);
   END LOOP;
--  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;   
END;
