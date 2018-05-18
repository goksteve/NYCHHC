DECLARE
   d_report_mon   DATE;
   n_cnt          PLS_INTEGER := 0;
BEGIN
    xl.open_log('TEST_PREPARE_DSRIP_TR18_REPORT', SYS_CONTEXT('USERENV','OS_USER')||': Generating DSRIP_TR18 reports', TRUE);

    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DDL';
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
   FOR i IN 1 .. 5
 LOOP
      d_report_mon := ADD_MONTHS (DATE '2018-01-01', i);
      DBMS_SESSION.set_identifier (d_report_mon);

      xl.begin_action('Deleting old data (if any) for '||d_report_mon);
      DELETE FROM TST_DSRIP_TR018_REPORT_QMED WHERE report_period_start_dt = d_report_mon;
      n_cnt := SQL%ROWCOUNT;
      COMMIT;
      xl.end_action(n_cnt||' rows deleted');
      etl.add_data (p_operation   => 'INSERT /*+ parallel(32) append */',
                    p_tgt         => 'TST_DSRIP_TR018_REPORT_QMED',
                    p_src         => 'V_TST_DSRIP_TR018_REPORT_QMED',
                    p_commit_at   => -1);
   END LOOP;
  xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;   
END;   