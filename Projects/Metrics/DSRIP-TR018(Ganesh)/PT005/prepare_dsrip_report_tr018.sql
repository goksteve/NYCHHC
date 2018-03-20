CREATE OR REPLACE PROCEDURE prepare_dsrip_report_tr018 AS
begin
  xl.open_log('PREPARE_DSRIP_REPORT_TR018', ': Generating DSRIP report TR018', TRUE);
  xl.begin_action('Truncating report tables for TR018');
  execute immediate 'truncate table dsrip_report_tr018_qmed';
  xl.end_action();
  etl.add_data
    (
      p_operation => 'INSERT /*+ parallel(32) */',
      p_tgt => 'DSRIP_REPORT_TR018_QMED',
      p_src => 'V_DSRIP_REPORT_TR018',
      p_commit_at => -1
    );
  xl.close_log('Successfully completed');  
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;
end;  
  
exit 0
