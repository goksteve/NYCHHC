CREATE OR REPLACE PACKAGE CDW.pkg_dsrip_reports AS
 /******************************************************************************
    NAME:       CREATE OR REPLACE PACKAGE BODY
    PURPOSE:

    REVISIONS:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        06/21/2018      goreliks1       1. Created this package.
 ******************************************************************************/
procedure sp_start_all;
 PROCEDURE sp_dsrip_tr002_023 ;
 PROCEDURE sp_dsrip_tr015;
 PROCEDURE sp_dsrip_tr017; 
 PROCEDURE sp_dsrip_tr022;
 PROCEDURE sp_dsrip_tr024_025;
 PROCEDURE sp_dsrip_tr044;
 PROCEDURE sp_dsrip_tr047;

END;
/




CREATE OR REPLACE PACKAGE BODY CDW.PKG_DSRIP_REPORTS AS
 /******************************************************************************
    NAME:       pkg_DSRIP_REPORTS
    PURPOSE: DSRIP REPORTS

    REVISIONS:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        06/21/2018      goreliks1       1. Created this package body.
 ******************************************************************************/


--**************** START PROCEDURE **********************

PROCEDURE sp_start_all AS
 n_step   VARCHAR2(255 BYTE);
BEGIN
--Author: GK
  n_step := 'DSRIP_REPORT_TR001';
  PT005.PREPARE_DSRIP_REPORT_TR001;

--Author: SG
  n_step := 'sp_dsrip_tr002_023';
  sp_dsrip_tr002_023;

--Author: GK  
  n_step := 'DSRIP_REPORT_TR006';
  pt005.prepare_dsrip_tr006_pqi90_rpt;

--Author: GK
  n_step := 'DSRIP_REPORT_TR010';
  PT005.PREPARE_DSRIP_REPORT_TR010;

--Author: GK
  n_step := 'DSRIP_REPORT_TR013_014';
  PT005.PREPARE_DSRIP_REPORT_TR013_014;

--Author: SG
  n_step := 'sp_dsrip_tr015';
  sp_dsrip_tr015;

--Author: GK
  n_step := 'DSRIP_REPORT_TR016';
  PT005.PREPARE_DSRIP_REPORT_TR016;

--Author: SG
  n_step := 'sp_dsrip_tr017';
  sp_dsrip_tr017;

--Author: GK
  n_step := 'DSRIP_REPORT_TR018';
  PT005.PREPARE_DSRIP_REPORT_TR018;

--Author: GK
  n_step := 'DSRIP_PQI90_REPORTS_7_8';
  PT005.PREPARE_PQI90_REPORTS_7_8;

--Author: SG
  n_step := 'sp_dsrip_tr022';
  sp_dsrip_tr022;

--Author: SG
  n_step := 'sp_dsrip_tr024-025';
  sp_dsrip_tr024_025;

--Author: GK
  n_step := 'PREPARE_DSRIP_REPORT_TR043';
  cdw.prepare_dsrip_report_tr043;

--Author: SG
  n_step := 'sp_dsrip_tr044';
  sp_dsrip_tr044;
EXCEPTION
 WHEN OTHERS THEN
  raise_application_error(
   -20001,
   'An error was encountered - ' || n_step || ' SQLCODE: ' || SQLCODE || ' -ERROR- ' || SQLERRM);
  RAISE;
END;

--****************  SP_DSRIP_TR002_023  *******************************

PROCEDURE sp_dsrip_tr002_023 AS
 -- 2018-MAY-23 SG Create
 BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

  xl.open_log('DSRIP_TR002_023_CDW_EPIC', 'DSRIP_TR002_023_CDW_EPIC', TRUE);

  xl.begin_action('RUNNING QCPR');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR002_023_A1C_CDW',
   p_src => 'V_DSRIP_TR002_023_A1C_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE QCPR');

  xl.begin_action('RUNNING EPIC');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR002_023_A1C_EPIC',
   p_src => 'V_DSRIP_TR002_023_A1C_EPIC',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE EPIC');

  xl.close_log('DSRIP_TR002_023 OK ');
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
 --****************  SP_DSRIP_TR015 ****************
 PROCEDURE sp_dsrip_tr015 AS
 -- 2018-july-23 SG Create
 BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

   xl.open_log('DSRIP_TR015_CARDIO_MON_CDW', 'DSRIP_TR015_CARDIO_MON_CDW', TRUE);
   xl.begin_action('RUNNING QCPR');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR015_CARDIO_MON_CDW',
   p_src => 'V_DSRIP_TR015_CARDIO_MON_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);
  xl.end_action('COMPLETE DSRIP_TR015_CARDIO_MON_CDW');
  xl.close_log('COMPLETE DSRIP_TR015_CARDIO_MON_CDW OK ');
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
 --**************** SP_DSRIP_TR017 *****************
 PROCEDURE sp_dsrip_tr017 AS
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
 --**************** SP_DSRIP_TR022 *****************

 PROCEDURE sp_dsrip_tr022 AS
 -- 2018-MAY-23 SG Create
 BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

   xl.open_log('DSRIP_TR_022_DIAB_SCREEN_CDW', 'DSRIP_TR_022_DIAB_SCREEN_CDW', TRUE);
   xl.begin_action('RUNNING QCPR');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR022_DIAB_SCREEN_CDW',
   p_src => 'V_DSRIP_TR022_DIAB_SCREEN_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);
  xl.end_action('COMPLETE DSRIP_TR_022_CDW');
  xl.close_log('COMPLETE DSRIP_TR_022 OK ');
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
 --****** sp_DSRIP_tr024_025 *********************

 PROCEDURE sp_dsrip_tr024_025 AS
 -- 2018-june-28 SG Create
 BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

  xl.open_log('SP_TR024_025_CDW', 'DSRIP_TR024_025_CDW', TRUE);

  xl.begin_action('RUNNING QCPR');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR024_025_CDW',
   p_src => 'V_DSRIP_TR024_025_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE DSRIP_TR024_025_CDW');
  xl.close_log('COMPLETE DSRIP_TR024_025_CDW...OK ');
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
--******************* DSRIP_TR044_STAT_CARDIO_CDW *************
PROCEDURE sp_dsrip_tr044 AS
 
 BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

  xl.open_log('sp_dsrip_tr_044', 'DSRIP_TR_044', TRUE);

  xl.begin_action('RUNNING QCPR');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR044_STAT_CARDIO_CDW',
   p_src => 'V_DSRIP_TR044_STAT_CARDIO_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE DSRIP_TR_044');
  xl.close_log('COMPLETE DSRIP_TR_044OK ');
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
--***********************************
PROCEDURE sp_dsrip_tr047 AS
BEGIN

 EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

 xl.open_log('sp_dsrip_tr_047', 'DSRIP_TR_047', TRUE);

 xl.begin_action('RUNNING QCPR');
 etl.add_data(
  p_operation => 'INSERT /*+ Parallel(32)  */',
  p_tgt => 'DSRIP_TR047_STAT_PHARM_CDW',
  p_src => 'V_DSRIP_TR047_STAT_PHARM_CDW',
  p_whr => 'Where 1= 1 ',
  p_commit_at => -1);

 xl.end_action('COMPLETE DSRIP_TR_047');
 xl.close_log('COMPLETE DSRIP_TR_047 OK ');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;




END pkg_dsrip_reports;
/