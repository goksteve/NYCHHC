CREATE OR REPLACE PROCEDURE PREPARE_DSRIP_REPORT_TR007(p_report_month IN DATE DEFAULT NULL)
IS  
   d_report_mon   DATE;
   n_cnt          PLS_INTEGER := 0;
BEGIN
  xl.open_log('PREPARE_DSRIP_REPORT_TR007', SYS_CONTEXT('USERENV','OS_USER')||': Generating DSRIP_TR007 reports', TRUE);

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DDL';
  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

  xl.begin_action('Setting the report month');
  d_report_mon := TRUNC(NVL(p_report_month, SYSDATE), 'MONTH');
  dbms_session.set_identifier(d_report_mon);
  xl.end_action('Set to '||d_report_mon);

  xl.begin_action('Deleting old TR007 data (if any) for '||d_report_mon);  
      
  DELETE FROM dsrip_report_tr007_pdi90 WHERE report_date = d_report_mon;
  n_cnt := n_cnt + SQL%ROWCOUNT;
  
--  DELETE FROM dsrip_report_results WHERE report_cd LIKE 'DSRIP-TR007%' AND period_start_dt = d_report_mon;
--  n_cnt := n_cnt + SQL%ROWCOUNT;
  
  COMMIT;
  xl.end_action(n_cnt||' rows deleted');  

--  dbms_session.set_identifier(d_report_mon);
  etl.add_data 
  (
    p_operation   => 'INSERT /*+ parallel(32) append */',
    p_tgt         => 'DSRIP_REPORT_TR007_PDI90',
    p_src         => 'V_DSRIP_REPORT_TR007_PDI90',
    p_commit_at   => -1
  );

--  etl.add_data
--  (
--    p_operation => 'INSERT',
--    p_tgt => 'DSRIP_REPORT_RESULTS',
--    p_src => 'SELECT 
--        ''DSRIP-TR007'' report_cd, 
--        report_date AS period_start_dt,
--        DECODE(GROUPING(network), 1, ''ALL networks'', network) network,
--        DECODE(GROUPING(facility_name), 1, ''ALL facilities'', facility_name) AS facility_name,
--        COUNT(1) AS denominator
--      FROM DSRIP_REPORT_TR007_PDI90 r
--      WHERE report_date = '''||d_report_mon||'''
--      GROUP BY GROUPING SETS((report_date, network, facility_name),(report_date))',
--    p_commit_at => -1
--  );   
  
xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;
/