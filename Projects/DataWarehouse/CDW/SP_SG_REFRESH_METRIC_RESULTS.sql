CREATE OR REPLACE PROCEDURE sp_sg_refresh_metric_results AS
BEGIN
 xl.open_log('SG_Metric_Results', 'Refreshing FACT_VISIT_METRIC_RESULTS', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

   BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE fact_visit_metric_results DROP CONSTRAINT pk_fact_visit_metric_results';
    EXECUTE IMMEDIATE 'DROP INDEX pk_fact_visit_metric_results';
   EXCEPTION
     WHEN OTHERS THEN
      NULL;
  END;

 EXECUTE IMMEDIATE 'TRUNCATE TABLE FACT_VISIT_METRIC_RESULTS';

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

  xl.begin_action('Setting the Network');
  dwm.set_parameter('NETWORK', r.network);
  xl.end_action(SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK'));

  etl.add_data(
   p_operation => 'INSERT /*+ APPEND PARALLEL(32) */',
   p_tgt => 'FACT_VISIT_METRIC_RESULTS',
   p_src => 'V_FACT_VISIT_METRIC_RESULTS',
   p_commit_at => -1);

  xl.end_action;
 END LOOP;


    BEGIN

     EXECUTE IMMEDIATE
      'CREATE UNIQUE INDEX pk_fact_visit_metric_results ON fact_visit_metric_results (visit_id, network) LOCAL PARALLEL 32';

     EXECUTE IMMEDIATE 'ALTER INDEX pk_fact_visit_metric_results NOPARALLEL';

     EXECUTE IMMEDIATE
      'ALTER TABLE fact_visit_metric_results ADD CONSTRAINT pk_fact_visit_metric_results PRIMARY KEY (visit_id, network) USING INDEX pk_fact_visit_metric_results';
    EXCEPTION
     WHEN OTHERS THEN
      NULL;
    END;
 xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);

  RAISE;
END;
/