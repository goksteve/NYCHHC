CREATE OR REPLACE PROCEDURE sp_sg_refresh_mat_views AS
BEGIN
 xl.open_log('refresh MV_FACT_VISITS_RESULTS_SUM', 'Refreshing mv_fact_visits_results_sum', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

 xl.begin_action('start refresh');
 NULL;
   DBMS_MVIEW.REFRESH('mv_fact_visits_results_sum', atomic_refresh => false);

 xl.close_log(' Refresh Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);

  RAISE;
END;
/