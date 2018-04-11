CREATE OR REPLACE PROCEDURE CDW.sp_gk_refresh_tr016_report AS
BEGIN
 xl.open_log('GK_DSRIP_REPORT_TR016', 'Refreshing dsrip_report_tr016', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

 EXECUTE IMMEDIATE 'TRUNCATE TABLE TST_GK_DSRIP_REPORT_TR016';

 FOR r IN (
           SELECT
            network
           FROM
           dim_hc_networks
          )
 LOOP
  xl.begin_action('Processing ' || r.network);

  xl.begin_action('Setting the Network');
  dwm.set_parameter('NETWORK', r.network);
  xl.end_action(SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK'));

  etl.add_data(
   p_operation => 'INSERT /*+ APPEND PARALLEL(32) */',
   p_tgt => 'TST_GK_DSRIP_REPORT_TR016',
   p_src => 'V_TST_GK_DSRIP_REPORT_TR016',
   p_commit_at => -1);

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
