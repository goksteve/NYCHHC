CREATE OR REPLACE PROCEDURE sp_sg_refresh_metric_results AS
 rcur   SYS_REFCURSOR;
 rec    cnf_dw_refresh%ROWTYPE;
BEGIN
 xl.open_log('SG_TST', 'Refreshing FACT_VISIT_METRIC_RESULTS', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

 FOR r IN (
           SELECT
           DISTINCT network
           FROM
           log_incremental_data_load
           WHERE
           table_name = 'FACT_RESULTS' AND max_cid > 0
          )
 LOOP
  xl.begin_action('Processing ' || r.network);
  dwm.set_parameter('NETWORK', r.network);

  etl.add_data(
   p_operation => 'INSERT /*+ APPEND PARALLEL(32) */',
   p_tgt => 'FACT_VISIT_METRIC_RESULTS',
   p_src => 'V_FACT_VISIT_METRIC_RESULTS',
   p_commit_at => -1);

  xl.end_action;
 END LOOP;

 xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);

  CLOSE rcur;

  RAISE;
END;
/