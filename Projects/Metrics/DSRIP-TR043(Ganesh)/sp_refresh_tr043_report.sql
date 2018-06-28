CREATE OR REPLACE PROCEDURE sp_refresh_tr043_report(p_report_month IN DATE DEFAULT NULL)
AS
  d_report_mon  DATE;
  n_cnt         PLS_INTEGER := 0;
BEGIN

 xl.open_log('PREPARE_TR043_REPORT', SYS_CONTEXT('USERENV','OS_USER')||': Generating TR043 reports', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
 
  xl.begin_action('Setting the report month');
  d_report_mon := TRUNC(NVL(p_report_month, SYSDATE), 'MONTH');
  dbms_session.set_identifier(d_report_mon);
  xl.end_action('Set to '||d_report_mon);
 
  xl.begin_action('Deleting old data (if any) for '||d_report_mon);
  DELETE FROM dsrip_tr043_bh_visits_report WHERE report_period_dt = d_report_mon;
  n_cnt := SQL%ROWCOUNT;
  
  COMMIT;
  xl.end_action(n_cnt||' rows deleted');

  etl.add_data 
  (
    p_operation   => 'INSERT /*+ PARALLEL(32) APPEND */',
    p_tgt         => 'DSRIP_TR043_BH_VISITS_REPORT',
    p_src         => 'V_DSRIP_TR043_BH_VISITS_REPORT',
    p_commit_at   => -1
  );
  
--  etl.add_data
--  (
--    p_operation => 'INSERT',
--    p_tgt => 'DSRIP_REPORT_RESULTS',
--    p_src => 'SELECT 
--        ''DSRIP-TR043'' report_cd, 
--        report_period_dt AS period_start_dt,
--        DECODE(GROUPING(network), 1, ''ALL networks'', network) network,
--        DECODE(GROUPING(facility_name), 1, ''ALL facilities'', facility_name) AS facility_name,
--        COUNT(1) denominator,
--        NULL numerator_1,
--        NULL numerator_2
--      FROM dsrip_tr043_bh_visits_report r
--      WHERE r.report_period_dt = '''||d_report_mon||'''
--      GROUP BY GROUPING SETS((report_period_dt, network, facility_name),(report_period_dt))',
--    p_commit_at => -1
--  );
        
  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;
/   


