CREATE OR REPLACE PROCEDURE prepare_dsrip_report_tr044(p_report_month IN DATE DEFAULT NULL) AS
/*
  15-Jun-2018, GK: Initial Version
*/
  d_report_mon  DATE;
  n_cnt         PLS_INTEGER;
BEGIN
  xl.open_log('PREPARE_DSRIP_REPORT_TR044', 'User '||SYS_CONTEXT('USERENV','OS_USER')||': Preparing data of the DSRIP report TR044', TRUE);
  
  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
  
  xl.begin_action('Setting the report month');
  d_report_mon := TRUNC(NVL(p_report_month, SYSDATE), 'MONTH');
  dbms_session.set_identifier(d_report_mon);
  xl.end_action('Set to '||d_report_mon);
  
  n_cnt := 0;
  
  xl.begin_action('Deleting old TR044 data (if any) for '||d_report_mon);

  DELETE FROM dsrip_report_tr044 WHERE report_dt = d_report_mon;
  n_cnt := n_cnt + SQL%ROWCOUNT;
  
  
  DELETE FROM dsrip_report_results WHERE report_cd LIKE 'DSRIP-TR044%' AND period_start_dt = d_report_mon;
  n_cnt := n_cnt + SQL%ROWCOUNT;
  
  COMMIT;
  xl.end_action(n_cnt||' rows deleted');
  
  etl.add_data
  (
    p_operation => 'INSERT /*+ parallel(32) */',
    p_tgt => 'DSRIP_REPORT_TR044',
    p_src => 'V_DSRIP_REPORT_TR044',
--    p_whr => 'WHERE rnum = 1',
    p_commit_at => -1
  );
  
  etl.add_data
  (
    p_operation => 'INSERT',
    p_tgt => 'DSRIP_REPORT_RESULTS',
    p_src => 'SELECT 
        ''DSRIP-TR044'' report_cd, 
        report_dt AS period_start_dt,
        DECODE(GROUPING(network), 1, ''ALL networks'', network) network,
        DECODE(GROUPING(facility_name), 1, ''ALL facilities'', facility_name) AS facility_name,
        COUNT(DISTINCT patient_id) denominator,
        SUM(CASE WHEN numerator_flag = ''Y'' THEN 1 ELSE 0 END) numerator_1
      FROM dsrip_report_tr044 r
      WHERE report_dt = '''||d_report_mon||'''
      GROUP BY GROUPING SETS((report_dt, network, facility_name),(report_dt))',
    p_commit_at => -1
  );
  
  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;
/