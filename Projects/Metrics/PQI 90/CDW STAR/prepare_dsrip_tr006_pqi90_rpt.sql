--DROP PROCEDURE PREPARE_PQI90_REPORTS;
--GRANT EXECUTE ON prepare_dsrip_tr006_pqi90_rpt TO CDW;

CREATE OR REPLACE PROCEDURE prepare_dsrip_tr006_pqi90_rpt(p_report_month IN DATE DEFAULT NULL) AS
/*
  2018-04-25, GK: created 
*/
  d_report_mon  DATE;
  n_cnt         PLS_INTEGER := 0;
BEGIN
  xl.open_log('PREPARE_PQI90_REPORTS', SYS_CONTEXT('USERENV','OS_USER')||': Generating PQI90 reports', TRUE);
  
  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
  
  xl.begin_action('Setting the report month');
  d_report_mon := TRUNC(NVL(p_report_month, SYSDATE), 'MONTH');
  dbms_session.set_identifier(d_report_mon);
  xl.end_action('Set to '||d_report_mon);
  
  xl.begin_action('Deleting old data (if any) for '||d_report_mon);
  DELETE FROM DSRIP_REPORT_TR006_PQI90 WHERE report_period_start_dt = d_report_mon;
  n_cnt := SQL%ROWCOUNT;
  
  DELETE FROM dsrip_report_results
  WHERE report_cd = 'PQI90' AND period_start_dt = d_report_mon;
  n_cnt := n_cnt + SQL%ROWCOUNT;
  
  COMMIT;
  xl.end_action(n_cnt||' rows deleted');
  
  etl.add_data
  (
    p_operation => 'INSERT /*+parallel(16)*/',
    p_tgt => 'DSRIP_REPORT_TR006_PQI90',
    p_src => 'V_DSRIP_REPORT_TR006_PQI90',
    p_commit_at => -1
  );
  
  etl.add_data
  (
    p_operation => 'INSERT',
    p_tgt => 'DSRIP_REPORT_RESULTS',
    p_src => 'SELECT 
        ''PQI90'' report_cd, 
        report_period_start_dt AS period_start_dt,
        DECODE(GROUPING(network), 1, ''ALL networks'', network) network,
        DECODE(GROUPING(facility), 1, ''ALL facilities'', facility) AS facility_name,
        COUNT(1) denominator
      FROM DSRIP_REPORT_TR006_PQI90 r
      WHERE r.report_period_start_dt = '''||d_report_mon||'''
      GROUP BY GROUPING SETS((report_period_start_dt, network, Facility),(report_period_start_dt))',
    p_commit_at => -1
  );
  
  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;
/