CREATE OR REPLACE PROCEDURE sp_dsrip_tr002_023_a1c_cdw AS
-- 2018-MAR-23 SG Create
BEGIN

 EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

 xl.open_log('dsrip_tr002_023_a1c_cdw', 'dsrip_tr002_023_a1c_cdw', TRUE);

 etl.add_data(
  p_operation => 'INSERT /*+ Parallel(32)  */',
  p_tgt => 'DSRIP_TR002_023_A1C_CDW',
  p_src => 'V_DSRIP_TR002_023_A1C_CDW',
  p_whr => 'Where 1= 1 ',
  p_commit_at => -1);

 xl.close_log('COMPLETE OK ');
EXCEPTION
 WHEN OTHERS THEN
  xl.close_log(SQLERRM, TRUE);
  RAISE;
END;