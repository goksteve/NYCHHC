DROP TABLE COMPASS_EDD_VOLUMES;
CREATE TABLE COMPASS_EDD_VOLUMES
(
  report_mnth           DATE,
--  NUM          NUMBER,
  METRIC_NAME  VARCHAR2(51 BYTE),
  BHC          NUMBER,
  CIH          NUMBER,
  HLM          NUMBER,
  JMC          NUMBER,
  KCH          NUMBER,
  LHC          NUMBER,
  MHC          NUMBER,
  NCB          NUMBER,
  WHH          NUMBER,
  ELM          NUMBER,
  QHC          NUMBER,
  ALL_FACILITIES        NUMBER
);
GRANT SElECT ON COMPASS_EDD_VOLUMES TO PUBLIC;

create or replace view V_COMPASS_EDD_VOLUMES
as
select report_mnth, metric_name, bhc, cih, hlm, jmc, kch, lhc, mhc, ncb, whh, elm, qhc, ALL_FACILITIES
from
(
  with dt
  as
  (
    SELECT --+ materialize
      NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_mnth
    FROM dual
  )
  select
    dt.report_mnth,
    m.num,
    m.metric_name,
    DECODE(GROUPING(f.FacilityCode), 1, 'All', f.FacilityCode)  FacilityCode, 
    SUM
    (
      case
        when m.metric_name = '# of Patients Arrived to ED'
          or m.metric_name = '# of Patients Left Before Triage' and bitand(v.progress_ind, 2) = 0 and v.Disposition_name in ('Unknown', 'Left Without Being Seen')
          or m.metric_name = '# of Patients Triaged' and bitand(v.progress_ind, 2) = 2
          or m.metric_name = '# of Patients Claimed by a Provider' and bitand(v.progress_ind, 6) = 6
          or m.metric_name = '# of Patients with a Disposition of LWBS' and v.disposition_name = 'Left Without Being Seen'
          or m.metric_name = '# of Patients Left After Triage' and bitand(v.progress_ind, 2) = 2 and (bitand(v.progress_ind, 8) = 0 or d.disposition_name = 'Left Without Being Seen')
          or m.metric_name = '# of Patients Left Without Being Seen' and (bitand(v.progress_ind, 8) = 0 or d.disposition_name = 'Left Without Being Seen')
          or m.metric_name = '# of Patients with a Disposition not LWBS' and bitand(v.progress_ind, 8) = 8
            and d.disposition_name not in ('Left Without Being Seen', 'Unknown')
          or m.metric_name = '# of Patients Left Against Medical Advice'
            and v.disposition_name = 'Left Against Medical Advice'
          or m.metric_name = '# of Patients Walked Out During Evaluation / Eloped'
            and d.disposition_name = 'Eloped'
          or m.metric_name = '# of Patients Seen '||CHR(38)||' Discharged' and d.disposition_class = 'DISCHARGED'
          or m.metric_name = '# of Patients Transferred to Another Hospital' and d.disposition_class = 'TRANSFERRED'
          or m.metric_name = '# of ED Patients Who Were Admitted' and d.disposition_class = 'ADMITTED'
        then num_of_visits
        else 0
      end
    ) metric_value
  from dt 
  join edd_fact_stats v on dt.report_mnth = trunc(v.visit_start_dt,'MONTH')
  join edd_dim_facilities f on f.FacilityKey = v.Facility_Key
  join edd_dim_dispositions d on d.disposition_name = v.disposition_name
  cross join
  (
    select 1 num, '# of Patients Arrived to ED' metric_name from dual union all
    select 2, '# of Patients Left Before Triage' from dual union all
    select 3, '# of Patients Triaged' from dual union all
    select 4, '# of Patients Claimed by a Provider' from dual union all
    select 5, '# of Patients with a Disposition of LWBS' from dual union all
    select 6, '# of Patients Left After Triage' from dual union all
    select 7, '# of Patients Left Without Being Seen' from dual union all
    select 8, '# of Patients with a Disposition not LWBS' from dual union all
    select 9, '# of Patients Left Against Medical Advice' from dual union all
    select 10, '# of Patients Walked Out During Evaluation / Eloped' from dual union all
    select 11, '# of Patients Seen '||CHR(38)||' Discharged' from dual union all
    select 12, '# of Patients Transferred to Another Hospital' from dual union all
    select 13, '# of ED Patients Who Were Admitted' from dual
  ) m
  where v.visit_start_dt >= dt.report_mnth and v.visit_start_dt < add_months(dt.report_mnth, 1)
--  and f.facilityKey not in (6, 11) 
  group by grouping sets((dt.report_mnth, m.num, m.metric_name, f.facilityCode), (dt.report_mnth, m.num, m.metric_name)) 
)
pivot
(
  max(metric_value)
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
);

CREATE OR REPLACE PROCEDURE SP_REFRESH_COMP_EDD_VOLUMES(p_report_month IN DATE DEFAULT NULL)
IS  
   d_report_mon   DATE;
   n_cnt          PLS_INTEGER := 0;
BEGIN
  xl.open_log('PREPARE_EDD_VOLUME_DATA', SYS_CONTEXT('USERENV','OS_USER')||': Generating EDD volume data', TRUE);

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DDL';
  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

  xl.begin_action('Setting the report month');
  d_report_mon := TRUNC(NVL(p_report_month, SYSDATE), 'MONTH');
  dbms_session.set_identifier(d_report_mon);
  xl.end_action('Set to '||d_report_mon);

  xl.begin_action('Deleting old EDD Volume data (if any) for '||d_report_mon);  
      
  DELETE FROM COMPASS_EDD_VOLUMES WHERE report_mnth = d_report_mon;
  n_cnt := n_cnt + SQL%ROWCOUNT;
  
  COMMIT;
  xl.end_action(n_cnt||' rows deleted');  

  etl.add_data 
  (
    i_operation   => 'INSERT /*+ parallel(32) append */',
    i_tgt         => 'COMPASS_EDD_VOLUMES',
    i_src         => 'V_COMPASS_EDD_VOLUMES',
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

--
--begin
--  for i in 0 .. 16
--  loop
--    dbms_output.put_line(add_months(date '2017-01-01', i));
--  end loop;
--
--end;



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
      SP_REFRESH_COMP_EDD_VOLUMES(v_report_mon);
   END LOOP;
--  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;   
END;

