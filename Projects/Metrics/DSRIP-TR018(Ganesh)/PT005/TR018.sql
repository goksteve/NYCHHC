-- Run every month:
whenever sqlerror exit 1
set feedback off

prompt Importing source data for the DSRIP report TR018 from 6 CDW databases ...
exec xl.open_log('DSRIP-TR018', 'Importing source data for the DSRIP report TR018', TRUE);

call xl.begin_action('Truncating staging tables for TR018');
truncate table dsrip_tr018_bp_results;
call xl.end_action();

@copy_table.sql BP_RESULTS

exec xl.close_log('Successfully completed');

prompt Generating DSRIP report TR018. It may take a while ...
set timi on

call prepare_dsrip_report_tr018();
  
exit 0

