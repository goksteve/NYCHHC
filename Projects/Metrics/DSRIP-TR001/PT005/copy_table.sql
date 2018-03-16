set arraysize 5000
set copycommit 2
set verify off
set echo off
set feedback off

define TABLE=DSRIP_TR001_&1

CALL xl.begin_action('Copying &TABLE data');

@@copy_from CBN
@@copy_from GP1
@@copy_from GP2
@@copy_from NBN
@@copy_from NBX
@@copy_from SMN

CALL xl.end_action();
