CREATE OR REPLACE PACKAGE BODY cdw.pkg_refresh_cdw_star_schema AS
 /******************************************************************************
    NAME:       PKG_REFRESH_CDW_CTAR_SCH
    PURPOSE:

    REVISIONS:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        05/10/2018      goreliks1       1. Created this package body.
 ******************************************************************************/

 PROCEDURE sp_start_refresh(p_refresh_type IN VARCHAR2, p_refresh_step VARCHAR2) IS
  -- *******  DESCRIPTION ******************************
  -- p_refresh_type  => 'F' - full  / 'I' - incremental
  -- p_refresh_step => 'ALL', 'REF','DIM', 'FACT', 'METRIC'
  --*****************************************************
  v_type   VARCHAR2(2) := p_refresh_type;
  v_step   VARCHAR2(10) := p_refresh_type;
 BEGIN
  IF v_type = 'F' THEN
   IF v_step = 'ALL' THEN
    -- dwm.refresh_data('where etl_step_num = 770');
    sp_refresh_ref_tables;
    sp_refresh_dim_tables;
   --   sp_refresh_fact_tables;

   END IF;
  END IF;
 END;

 PROCEDURE sp_refresh_ref_tables IS
 BEGIN

  dwm.refresh_data('where etl_step_num >=710 and etl_step_num <=770');
 END;

 PROCEDURE sp_refresh_dim_tables IS
 BEGIN

  dwm.refresh_data('where etl_step_num >=820 and etl_step_num <=1020');
 END;

 PROCEDURE sp_refresh_fact_tables IS
 BEGIN

  dwm.refresh_data('where etl_step_num >=0 and etl_step_num <=0');
 END;

 PROCEDURE sp_refresh_fact_results_full IS
  v_table   VARCHAR2(100) := 'FACT_RESULTS';
  n_cnt     NUMBER;
 BEGIN

  xl.open_log('sp_refresh_fact_results_full ', 'STARTING sp_refresh_fact_results_full', TRUE);
  xl.begin_action('Processing ' || 'updating log_incremental_data_load');
  UPDATE
   log_incremental_data_load
  SET
   max_cid = 0
  WHERE
   table_name = 'FACT_RESULTS';
 xl.end_action;
 xl.begin_action('Processing ' || 'Rename table');
  EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table || ' RENAME TO ' || v_table || '_STG1';
  EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table || '_STG  ' || 'RENAME TO ' || v_table;
  EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table || '_STG1 ' || ' RENAME TO ' || v_table || '_STG';
 xl.end_action;
 xl.begin_action('Processing ' || 'start REFRESH FACT_RESULT');
  SELECT
   COUNT(1)
  INTO
   n_cnt
  FROM
   fact_results_stg
  WHERE
   ROWNUM < 100;

  IF n_cnt > 0 THEN
   xl.begin_action('Processing ' || 'start REFRESH FACT_RESULT');
   dwm.refresh_data('where etl_step_num >= 3110 and etl_step_num <= 3160');
   xl.end_action;
   xl.begin_action('Processing ' || 'REBUILD INDEXES/TRIGGERS');
   BEGIN
      EXECUTE IMMEDIATE 'ALTER TABLE FACT_RESULTS_STG DROP CONSTRAINT PK_FACT_RESULTS';
      EXECUTE IMMEDIATE 'DROP INDEX PK_FACT_RESULTS';
      EXECUTE IMMEDIATE 'DROP trigger tr_insert_fact_results';
    EXCEPTION
    WHEN OTHERS THEN
    NULL;
   END;

   ---  CREATE INDEXEX AND TRIGGERS BACK ----------
   -- Indexes ---
   EXECUTE IMMEDIATE
    'CREATE UNIQUE INDEX pk_fact_results  ON fact_results( visit_id,event_id,data_element_id,result_report_number,multi_field_occurrence_number,item_number,network) LOCAL PARALLEL 32 ';
   EXECUTE IMMEDIATE 'ALTER INDEX pk_fact_results  NOPARALLEL ';
   EXECUTE IMMEDIATE
    'ALTER TABLE fact_results ADD CONSTRAINT pk_fact_results PRIMARY KEY(visit_id, event_id, data_element_id, result_report_number, multi_field_occurrence_number, item_number,network) USING INDEX pk_fact_results';
   -- Tirgger
   EXECUTE IMMEDIATE
       'CREATE OR REPLACE TRIGGER tr_insert_fact_results '
    || 'FOR INSERT OR UPDATE '
    || 'ON fact_results  '
    || 'COMPOUND TRIGGER  '
    || 'BEFORE STATEMENT IS '
    || 'BEGIN '
    || ' dwm.init_max_cids(''FACT_RESULTS''); '
    || 'END BEFORE STATEMENT;  '
    || 'AFTER EACH ROW IS '
    || 'BEGIN '
    || '  dwm.max_cids(:new.network) := GREATEST(dwm.max_cids(:new.network), :new.cid); '
    || 'END AFTER EACH ROW; '
    || ' AFTER STATEMENT IS '
    || ' BEGIN '
    || ' dwm.record_max_cids(''FACT_RESULTS''); '
    || 'END AFTER STATEMENT; '
    || ' END tr_insert_fact_results; ';
xl.end_action;
---- update  log_incremental_data_load table
 
BEGIN
 xl.begin_action('Processing ' || 'UPDATE  log_incremental_data_load with max CID');
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'CBN';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'GP1';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'GP2';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'NBN';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'NBX';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'QHN';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'SBN';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'SMN';
COMMIT;
xl.end_action;
END;
xl.end_action;

  END IF;
xl.close_log(' sp_refresh_fact_results_full Successfully completed');

EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);
 END;

 PROCEDURE sp_refresh_fact_metric_tables IS
 BEGIN

  dwm.refresh_data('where etl_step_num >=0 and etl_step_num <=0');
 END;


END pkg_refresh_cdw_star_schema;
/