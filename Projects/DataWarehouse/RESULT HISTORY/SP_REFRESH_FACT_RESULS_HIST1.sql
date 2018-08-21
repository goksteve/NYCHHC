CREATE OR REPLACE PROCEDURE SP_REFRESH_FACT_RESULTS_HIST AS
BEGIN
 xl.open_log('FACT_RESULTS_HIST', 'Refreshing FACT_RESULTS_HIST', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

   BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE FACT_RESULTS_HIST DROP CONSTRAINT pk_fact_results_hist';
    EXECUTE IMMEDIATE 'DROP INDEX pk_fact_results_hist';
    EXECUTE IMMEDIATE 'DROP INDEX ui_fact_results_hist_key';
   EXCEPTION
     WHEN OTHERS THEN
      NULL;
  END;

 EXECUTE IMMEDIATE 'TRUNCATE TABLE FACT_RESULTS_HIST';

 FOR r IN (
           SELECT
           DISTINCT network
           FROM
           DIM_HC_NETWORKS
          )
 LOOP
  xl.begin_action('Processing ' || r.network);
  dwm.set_parameter('NETWORK', r.network);
  xl.end_action( 'Current Network: ' ||SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK'));

  xl.begin_action('Processing data from result history table');
  etl.add_data(
   p_operation => 'INSERT /*+ APPEND PARALLEL(48) */',
   p_tgt => 'FACT_RESULTS_HIST',
   p_src => 'V_FACT_RESULTS_HIST',
   p_commit_at => -1);
  xl.end_action(' End  data from RESULT_HISTORY table');

  xl.begin_action('Processing data from  PROC_EVENT_HISTORY table ');
   etl.add_data(
   p_operation => 'INSERT /*+ APPEND PARALLEL(48) */',
   p_tgt => 'FACT_RESULTS_HIST',
   p_src => 'V_STG_PROC_RESULTS_HIST',
   p_commit_at => -1);
 xl.end_action ('End  data from PROC_EVENT_HISTORY table');

 END LOOP;
BEGIN
    EXECUTE IMMEDIATE
     'CREATE UNIQUE INDEX pk_fact_results_hist ON fact_results_hist( visit_id,   event_id,  data_element_id,  result_report_number,  multi_field_occurrence_number,  item_number,  network) LOCAL PARALLEL 32';
    EXECUTE IMMEDIATE 'ALTER INDEX pk_fact_results_hist NOPARALLEL';
    EXECUTE IMMEDIATE
    'ALTER TABLE fact_results_hist  ADD CONSTRAINT pk_fact_results_hist PRIMARY KEY(visit_id,event_id,data_element_id,result_report_number,multi_field_occurrence_number,item_number,network)USING INDEX pk_fact_results_hist';
   EXECUTE IMMEDIATE 'CREATE INDEX ui_fact_results_hist_key ON CDW.fact_results_hist(VISIT_KEY) PARALLEL 32';
   EXECUTE IMMEDIATE ' ALTER INDEX ui_fact_results_hist_key NOPARALLEL';
 EXCEPTION
     WHEN OTHERS THEN
      NULL;
    END;

 xl.close_log('SP_REFRESH_FACT_RESULTS_HIST Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);

  RAISE;
END;
/