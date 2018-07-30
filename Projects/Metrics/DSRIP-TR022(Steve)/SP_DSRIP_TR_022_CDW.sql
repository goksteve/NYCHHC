CREATE OR REPLACE PROCEDURE sp_dsrip_tr_022_diab_scr_cdw AS
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