CREATE OR REPLACE PACKAGE pkg_dsrip_reports AS
 /******************************************************************************
    NAME:       CREATE OR REPLACE PACKAGE BODY
    PURPOSE:

    REVISIONS:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        06/21/2018      goreliks1       1. Created this package.
 ******************************************************************************/

 PROCEDURE sp_dsrip_tr_022;

 PROCEDURE sp_dsrip_tr_017;

 PROCEDURE sp_dsrip_tr_024;

END;
/

CREATE OR REPLACE PACKAGE BODY pkg_dsrip_reports AS
 /******************************************************************************
    NAME:       pkg_DSRIP_REPORTS
    PURPOSE: DSRIP REPORTS

    REVISIONS:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        06/21/2018      goreliks1       1. Created this package body.
 ******************************************************************************/

 PROCEDURE sp_dsrip_tr_022 AS
 -- 2018-MAY-23 SG Create
 BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

  xl.open_log('DSRIP_TR_022_DIAB_SCREEN_CDW', 'DSRIP_TR_022_DIAB_SCREEN_CDW', TRUE);

  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR_022_DIAB_SCREEN_CDW',
   p_src => 'V_DSRIP_TR_022_DIAB_SCREEN_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.close_log('COMPLETE DSRIP_TR_022 OK ');
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;

 --**************** SP_DSRIP_TR_017 *****************
 PROCEDURE sp_dsrip_tr_017 AS
 -- 2018-MAY-23 SG Create
 BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

  xl.open_log('DSRIP_TR_017_CDW_EPIC', 'DSRIP_TR_017_CDW_EPIC', TRUE);

  xl.begin_action('RUNNING QCPR');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR_017_DIAB_MON_CDW',
   p_src => 'V_DSRIP_TR_017_DIAB_MON_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE QCPR');

  xl.begin_action('RUNNING EPIC');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR_017_DIAB_MON_EPIC',
   p_src => 'V_DSRIP_TR_017_DIAB_MON_EPIC',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE EPIC');

  xl.close_log('COMPLETE DSRIP_TR_017 OK ');
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;

 --****** sp_DSRIP_tr_024 *********************

 PROCEDURE sp_dsrip_tr_024 AS
 -- 2018-june-28 SG Create
 BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

  xl.open_log('SP_TR_024_AMM_CDW', 'DSRIP_TR_024_AMM_CDW', TRUE);

  xl.begin_action('RUNNING QCPR');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR_024_AMM_CDW',
   p_src => 'V_DSRIP_TR_024_AMM_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE DSRIP_TR_024_AMM_CDW');
  xl.close_log('COMPLETE DSRIP_TR_024_AMM_CDW OK ');
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;

END pkg_dsrip_reports;
/