CREATE TABLE COMPASS_EDD_ESI_VISIT_CNTS
(
  report_mnth           DATE,
  ESI    VARCHAR2(1061 BYTE),
  BHC    NUMBER,
  CIH    NUMBER,
  HLM    NUMBER,
  JMC    NUMBER,
  KCH    NUMBER,
  LHC    NUMBER,
  MHC    NUMBER,
  NCB    NUMBER,
  WHH    NUMBER,
  ELM    NUMBER,
  QHC    NUMBER,
  ALL_FACILITIES    NUMBER
);
GRANT SELECT ON COMPASS_EDD_ESI_VISIT_CNTS TO PUBLIC;



CREATE OR REPLACE VIEW V_COMPASS_EDD_ESI_VISIT_CNTS 
AS
select
  report_mnth, esi, bhc, cih, hlm, jmc, kch, lhc, mhc, ncb, whh, elm, qhc, ALL_FACILITIES
from
(
  select
    NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(date '2017-01-01', 'MONTH')) as report_mnth, 
    e.esiKey, e.esi||' (ESI '||e.esiKey||') - # of Visits' esi,
    decode(grouping(f.FacilityCode), 1, 'All', f.FacilityCode) FacilityCode, 
    sum(nvl(v.num_of_visits, 0)) num_of_visits
  from edd_dim_facilities f 
  cross join edd_dim_esi e
  left join edd_fact_stats v
    on v.esi_key = e.esiKey
   and v.Facility_Key = f.FacilityKey
--   and v.visit_start_dt >= date '2017-01-01' and v.visit_start_dt < date '2017-02-01'
   and v.visit_start_dt >= NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(date '2017-01-01', 'MONTH')) 
   and v.visit_start_dt < add_months(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(date '2017-01-01', 'MONTH')), 1)   
   and bitand(v.progress_ind, 32) = 32
  where e.esikey > 0
  group by grouping sets((NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(date '2017-01-01', 'MONTH')), e.esiKey, e.esi, f.FacilityCode),(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(date '2017-01-01', 'MONTH')), e.esiKey, e.esi))
)
pivot
(
  max(num_of_visits)
  for FacilityCode in 
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
    'All' as ALL_FACILITIES
  )
)
order by esikey;



CREATE OR REPLACE PROCEDURE SP_REFRESH_COMPASS_ESI_VST_CNT(p_report_month IN DATE DEFAULT NULL)
IS  
   d_report_mon   DATE;
   n_cnt          PLS_INTEGER := 0;
BEGIN
  xl.open_log('PREPARE_EDD_ESI_VST_CNTS', SYS_CONTEXT('USERENV','OS_USER')||': Generating EDD visit counts per each ESI category', TRUE);

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DDL';
  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

  xl.begin_action('Setting the report month');
  d_report_mon := TRUNC(NVL(p_report_month, SYSDATE), 'MONTH');
  dbms_session.set_identifier(d_report_mon);
  xl.end_action('Set to '||d_report_mon);

  xl.begin_action('Deleting old EDD visit counts (if any) for '||d_report_mon);  
      
  DELETE FROM COMPASS_EDD_ESI_VISIT_CNTS WHERE report_mnth = d_report_mon;
  n_cnt := n_cnt + SQL%ROWCOUNT;
  
  COMMIT;
  xl.end_action(n_cnt||' rows deleted');  

  etl.add_data 
  (
    i_operation   => 'INSERT /*+ parallel(32) append */',
    i_tgt         => 'COMPASS_EDD_ESI_VISIT_CNTS',
    i_src         => 'V_COMPASS_EDD_ESI_VISIT_CNTS',
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
      SP_REFRESH_COMPASS_ESI_VST_CNT(v_report_mon);
   END LOOP;
--  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;   
END;



