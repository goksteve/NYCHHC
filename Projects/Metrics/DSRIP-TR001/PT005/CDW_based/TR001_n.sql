-- Run every month:
whenever sqlerror exit 1
set feedback off

prompt Generating DSRIP report TR001. It may take a while ...
set timi on
call prepare_dsrip_report_tr001_n();

exit 0