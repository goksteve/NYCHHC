CREATE OR REPLACE PROCEDURE sp_refresh_tr13_14_report AS
BEGIN
 xl.open_log('DSRIP_REPORT_TR13_14', 'Refreshing dsrip_report_tr13_14', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

 EXECUTE IMMEDIATE 'TRUNCATE TABLE DSRIP_REPORT_TR13_14';

-- FOR r IN (
--           SELECT
--            network
--           FROM
--           dim_hc_networks
--          )
-- LOOP
--  xl.begin_action('Processing ' || r.network);
--
--  xl.begin_action('Setting the Network');
--  dwm.set_parameter('NETWORK', r.network);
--  xl.end_action(SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK'));

  etl.add_data(
   p_operation => 'INSERT /*+ APPEND PARALLEL(32) */',
   p_tgt => 'DSRIP_REPORT_TR13_14',
   p_src => 'V_DSRIP_REPORT_TR13_14',
   p_commit_at => -1);

--  xl.end_action;
-- END LOOP;


xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);

  RAISE;
END;
/
