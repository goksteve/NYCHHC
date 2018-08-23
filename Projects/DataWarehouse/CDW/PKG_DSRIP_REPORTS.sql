CREATE OR REPLACE PACKAGE CDW.pkg_dsrip_reports AS
 /******************************************************************************
    NAME:       CREATE OR REPLACE PACKAGE BODY
    PURPOSE:

    REVISIONS:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        06/21/2018      goreliks1       1. Created this package.
 ******************************************************************************/

  PROCEDURE sp_start_all;
  PROCEDURE sp_dsrip_tr001;
  PROCEDURE sp_dsrip_tr002_023;
  PROCEDURE sp_dsrip_tr006;
  PROCEDURE sp_dsrip_tr007;
  PROCEDURE sp_dsrip_tr010;  
  PROCEDURE sp_dsrip_tr013_014;
  PROCEDURE sp_dsrip_tr015;
  PROCEDURE sp_dsrip_tr016;  
  PROCEDURE sp_dsrip_tr017; 
  PROCEDURE sp_dsrip_tr018; 
  PROCEDURE sp_dsrip_tr022;
  PROCEDURE sp_dsrip_tr024_025;
  PROCEDURE SP_DSRIP_TR026;
  PROCEDURE sp_dsrip_tr043;
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
  sp_dsrip_tr001;

--Author: SG
  n_step := 'sp_dsrip_tr002_023';
  sp_dsrip_tr002_023;

--Author: GK  
  n_step := 'DSRIP_REPORT_TR006';
  sp_dsrip_tr006;

--Author: GK
  n_step := 'DSRIP_REPORT_TR007';
  sp_dsrip_tr007;

--Author: GK
  n_step := 'DSRIP_REPORT_TR010';
  sp_dsrip_tr010;

--Author: GK
  n_step := 'DSRIP_REPORT_TR013_014';
  sp_dsrip_tr013_014;

--Author: SG
  n_step := 'sp_dsrip_tr015';
  sp_dsrip_tr015;

--Author: GK
  n_step := 'DSRIP_REPORT_TR016';
  sp_dsrip_tr016;

--Author: SG
  n_step := 'sp_dsrip_tr017';
  sp_dsrip_tr017;

--Author: GK
  n_step := 'DSRIP_REPORT_TR018';
  sp_dsrip_tr018;

--Author: GK
  n_step := 'DSRIP_PQI90_REPORTS_7_8';
  PT005.PREPARE_PQI90_REPORTS_7_8;

--Author: SG
  n_step := 'SP_DSRIP_TR022';
  sp_dsrip_tr022;

--Author: SG
  n_step := 'SP_DSRIP_TR024-025';
  sp_dsrip_tr024_025;

--Author: SG
  n_step := 'SP_DSRIP_TR026_CDW';
  SP_DSRIP_TR026;

--Author: GK
  n_step := 'PREPARE_DSRIP_REPORT_TR043';
  sp_dsrip_tr043;

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

 --****************  SP_DSRIP_TR001 *******************************

PROCEDURE sp_dsrip_tr001 AS
 BEGIN
    PT005.PREPARE_DSRIP_REPORT_TR001;
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
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
 
 
  --****************  SP_DSRIP_TR006 *******************************

PROCEDURE sp_dsrip_tr006 AS
 BEGIN
    PT005.PREPARE_DSRIP_TR006_PQI90_RPT;
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
 
 
 --****************  SP_DSRIP_TR007 *******************************

PROCEDURE sp_dsrip_tr007 AS
 -- 2018-MAY-23 SG Create
 BEGIN
  PREPARE_DSRIP_REPORT_TR007;
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
 

  --****************  SP_DSRIP_TR010 *******************************

PROCEDURE sp_dsrip_tr010 AS
 BEGIN
    pt005.prepare_dsrip_report_tr010;
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
 
 
  --****************  SP_DSRIP_TR013_014 *******************************

PROCEDURE sp_dsrip_tr013_014 AS
 BEGIN
    pt005.prepare_dsrip_report_tr013_014;
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
 
  --****************  SP_DSRIP_TR016 *******************************

PROCEDURE sp_dsrip_tr016 AS
 BEGIN
    pt005.prepare_dsrip_report_tr016;
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
   p_tgt => 'DSRIP_TR017_DIAB_MON_CDW',
   p_src => 'V_DSRIP_TR017_DIAB_MON_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE QCPR');

  xl.begin_action('RUNNING EPIC');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR017_DIAB_MON_EPIC',
   p_src => 'V_DSRIP_TR017_DIAB_MON_EPIC',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE EPIC');

  xl.close_log('COMPLETE DSRIP_TR_017 OK ');
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
 
  --****************  SP_DSRIP_TR018 *******************************

PROCEDURE sp_dsrip_tr018 AS
 BEGIN
    pt005.prepare_dsrip_report_tr018;
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
 --****** SP_DSRIP_TR024_025 *********************

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

 

   --****************  SP_DSRIP_TR043 *******************************

PROCEDURE sp_dsrip_tr043 AS
 BEGIN
    cdw.prepare_dsrip_report_tr043;
 EXCEPTION
  WHEN OTHERS THEN
   xl.close_log(SQLERRM, TRUE);
   RAISE;
 END;
 
    
    
--***************  SP_DSRIP_TR026 ****************************
 PROCEDURE SP_DSRIP_TR026 AS
 -- 2018-AUG-24 SG Create
 BEGIN

  EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

  xl.open_log('SP_DSRIP_TR026_CDW', 'SP_DSRIP_TR026_CDW', TRUE);

  xl.begin_action('RUNNING QCPR');
  etl.add_data(
   p_operation => 'INSERT /*+ Parallel(32)  */',
   p_tgt => 'DSRIP_TR026_APD_CDW',
   p_src => 'V_DSRIP_TR026_APD_CDW',
   p_whr => 'Where 1= 1 ',
   p_commit_at => -1);

  xl.end_action('COMPLETE DSRIP_TR026_APD_CDW');
  xl.close_log('COMPLETE DSRIP_TR026_APD_CDW...OK ');

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