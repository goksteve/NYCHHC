CREATE OR REPLACE PROCEDURE sp_sg_refresh_metric_results AS
BEGIN
  xl.open_log('SG_TST', 'Refreshing FACT_VISIT_METRIC_RESULTS', TRUE);

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

  FOR r IN 
  (
    SELECT DISTINCT network
    FROM log_incremental_data_load
    WHERE table_name = 'FACT_RESULTS' AND max_cid > 0
  )
  LOOP
    xl.begin_action('Processing ' || r.network);
    
    xl.begin_action('Setting the Network');
    dwm.set_parameter('NETWORK', r.network);
    xl.end_action(sys_context('CTX_CDW_MAINTENANCE','NETWORK'));

    etl.add_data
    (
      p_operation => 'INSERT /*+ APPEND PARALLEL(32) */',
      p_tgt => 'FACT_VISIT_METRIC_RESULTS',
      p_src => 'V_FACT_VISIT_METRIC_RESULTS',
      p_commit_at => -1
    );

    xl.end_action;
  END LOOP;

  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);

  RAISE;
END;
/