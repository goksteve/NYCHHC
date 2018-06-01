BEGIN
  xl.open_log('PREPARE_DSRIP_REPORT_TR043', 'User '||SYS_CONTEXT('USERENV','OS_USER')||': Preparing data of the DSRIP report TR043', TRUE);
  etl.add_data
  (
    p_operation => 'INSERT /*+ APPEND PARALLEL(32) */',
    p_tgt => 'DSRIP_TR043_BH_VISITS_REPORT',
    p_src => 'V_DSRIP_TR043_BH_VISITS_REPORT',
    p_commit_at => -1
  );
  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;