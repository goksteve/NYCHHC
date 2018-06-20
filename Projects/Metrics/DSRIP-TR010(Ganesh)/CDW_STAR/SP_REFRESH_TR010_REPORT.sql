CREATE OR REPLACE PROCEDURE SP_REFRESH_TR010_REPORT(p_report_month IN DATE DEFAULT NULL)  
AS
  d_report_mon  DATE;
  n_cnt         PLS_INTEGER;
BEGIN
 xl.open_log('DSRIP_REPORT_TR010', 'Refreshing dsrip_report_tr010', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

  xl.begin_action('Setting the report month');
  d_report_mon := TRUNC(NVL(p_report_month, SYSDATE), 'MONTH');
  dbms_session.set_identifier(d_report_mon);
  xl.end_action('Set to '||d_report_mon);
  
  xl.begin_action('Deleting old TR010 data (if any) for '||d_report_mon);

  DELETE FROM dsrip_report_tr010 WHERE report_dt = d_report_mon;
  n_cnt := n_cnt + SQL%ROWCOUNT;
  
  DELETE FROM dsrip_report_results WHERE report_cd LIKE 'DSRIP-TR010%' AND period_start_dt = d_report_mon;
  n_cnt := n_cnt + SQL%ROWCOUNT;
  
  COMMIT;
  xl.end_action(n_cnt||' rows deleted');
  

  etl.add_data
  (
    p_operation => 'INSERT /*+ APPEND PARALLEL(32) */',
    p_tgt => 'DSRIP_REPORT_TR016',
    p_src => 'V_DSRIP_REPORT_TR016',
    p_commit_at => -1
  );
  
  etl.add_data
  (
    p_operation => 'INSERT',
    p_tgt => 'DSRIP_REPORT_RESULTS',
    p_src => 'SELECT 
        ''DSRIP-TR010'' report_cd, 
        report_dt AS period_start_dt,
        DECODE(GROUPING(network), 1, ''ALL networks'', network) network,
        DECODE(GROUPING(facility_name), 1, ''ALL facilities'', facility_name) AS facility_name,
        COUNT(CASE WHEN numerator_flag = ''Y'' THEN 1 END ) AS numerator_1
      FROM dsrip_report_tr010 r
      WHERE report_period_start_dt = '''||d_report_mon||'''
      GROUP BY GROUPING SETS((report_dt, network, facility_name),(report_dt))',
    p_commit_at => -1
  );  

xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;
/
