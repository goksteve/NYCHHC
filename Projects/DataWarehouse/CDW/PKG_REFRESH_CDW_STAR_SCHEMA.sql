CREATE OR REPLACE PACKAGE pkg_refresh_cdw_star_schema AS
 /*
   Package REFRESH_CDW_FACT_DIMS contains procedures for performing data refresh:


   History of changes (newest to oldest):
   ------------------------------------------------------------------------------
   09-MAY-2018, SG: created package

 */

 -- Procedure SP_START_REFRESH to start refresh process, in the order of REF, DIM, FACT and METRIC.
 PROCEDURE sp_start_refresh(p_refresh_type IN VARCHAR2, p_refresh_step VARCHAR2);

 -- Procedure SP_REFRESH_REF_TABLES to refresh reference tables
 PROCEDURE sp_refresh_ref_tables;

 -- Procedure SP_REFRESH_DIM_TABLES to refresh the dimension tables
 PROCEDURE sp_refresh_dim_tables;

 -- Procedure SP_REFRESH_FACT_TABLES to refresh the fact tables
 PROCEDURE sp_refresh_fact_tables;

 -- Procedure SP_REFRESH_METRIC_TABLES to refresh the fact-metric tables
 PROCEDURE sp_ref_fact_visit_metric_rslt;

 -- Procedure SP_REFRESH_FACT_RREULTS_FULL to refresh the fact-metric tables
 PROCEDURE sp_refresh_fact_results_full;

PROCEDURE sp_refresh_patient_metric_diag;

END pkg_refresh_cdw_star_schema;
/

--*********************************************

CREATE OR REPLACE PACKAGE BODY PKG_REFRESH_CDW_STAR_SCHEMA AS
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
--  v_table   VARCHAR2(100) := 'FACT_RESULTS';
--  n_cnt     NUMBER;
 BEGIN
  UPDATE
   log_incremental_data_load
  SET
   max_cid = 0
  WHERE
   table_name = 'FACT_RESULTS';
   COMMIT;
--  EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table || ' RENAME TO ' || v_table || '_STG1';
--  EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table || '_STG  ' || 'RENAME TO ' || v_table;
--  EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table || '_STG1 ' || ' RENAME TO ' || v_table || '_STG';
--  SELECT
--   COUNT(1)
--  INTO
--   n_cnt
--  FROM
--   fact_results_stg
--  WHERE
--   ROWNUM < 100;
--************   DROP INDEXES /TRIGGERES **************
    BEGIN
     EXECUTE IMMEDIATE 'ALTER TABLE FACT_RESULTS_STG DROP CONSTRAINT PK_FACT_RESULTS';
    EXCEPTION
     WHEN OTHERS THEN
      NULL;
    END;

    BEGIN
     EXECUTE IMMEDIATE 'DROP INDEX PK_FACT_RESULTS';
    EXCEPTION
     WHEN OTHERS THEN
      NULL;
    END;

    BEGIN
     EXECUTE IMMEDIATE 'DROP trigger tr_insert_fact_results';
    EXCEPTION
     WHEN OTHERS THEN
      NULL;
    END;
--*************  LOAD TABLE  ***************************
-- IF n_cnt > 0 THEN
   dwm.refresh_data('where etl_step_num = 3110');
   dwm.refresh_data('where etl_step_num = 3120');
   dwm.refresh_data('where etl_step_num = 3130');
   dwm.refresh_data('where etl_step_num = 3140');
   dwm.refresh_data('where etl_step_num = 3150');
   dwm.refresh_data('where etl_step_num = 3160' );
--******************************************************

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

---- update  log_incremental_data_load table
 
BEGIN
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'CBN';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'GP1';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'GP2';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'NBN';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'NBX';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'QHN';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'SBN';
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  SET max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) WHERE table_name = 'FACT_RESULTS' AND network  = 'SMN';
COMMIT;

EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);
 END;

END;


 PROCEDURE sp_ref_fact_visit_metric_rslt IS
BEGIN
 xl.open_log('SG_Metric_Results', 'Refreshing FACT_VISIT_METRIC_RESULTS', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

   BEGIN
    EXECUTE IMMEDIATE 'ALTER TABLE fact_visit_metric_results DROP CONSTRAINT pk_fact_visit_metric_results';
    EXECUTE IMMEDIATE 'DROP INDEX pk_fact_visit_metric_results';
   EXCEPTION
     WHEN OTHERS THEN
      NULL;
  END;

 EXECUTE IMMEDIATE 'TRUNCATE TABLE FACT_VISIT_METRIC_RESULTS';

 FOR r IN (
           SELECT
           DISTINCT network
           FROM
           log_incremental_data_load
           WHERE
           table_name = 'FACT_RESULTS' AND max_cid > 0
          )
 LOOP
  xl.begin_action('Processing ' || r.network);

  xl.begin_action('Setting the Network');
  dwm.set_parameter('NETWORK', r.network);
  xl.end_action(SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK'));

  etl.add_data(
   p_operation => 'INSERT /*+ APPEND PARALLEL(48) */',
   p_tgt => 'FACT_VISIT_METRIC_RESULTS',
   p_src => 'V_FACT_VISIT_METRIC_RESULTS',
   p_commit_at => -1);

  xl.end_action;
 END LOOP;


    BEGIN

     EXECUTE IMMEDIATE
      'CREATE UNIQUE INDEX pk_fact_visit_metric_results ON fact_visit_metric_results (visit_id, network) LOCAL PARALLEL 32';

     EXECUTE IMMEDIATE 'ALTER INDEX pk_fact_visit_metric_results NOPARALLEL';

     EXECUTE IMMEDIATE
      'ALTER TABLE fact_visit_metric_results ADD CONSTRAINT pk_fact_visit_metric_results PRIMARY KEY (visit_id, network) USING INDEX pk_fact_visit_metric_results';
    EXCEPTION
     WHEN OTHERS THEN
      NULL;
    END;
 xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);

  RAISE;
END;


PROCEDURE sp_refresh_patient_metric_diag AS
BEGIN
 xl.open_log('Sg_Patient_Metric_Results', 'Refreshing FACT_PATIENT_METRIC_DIAG', TRUE);

 EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
 EXECUTE IMMEDIATE 'TRUNCATE TABLE FACT_PATIENT_METRIC_DIAG';

 FOR r IN (
           SELECT
           DISTINCT network
           FROM
           DIM_HC_NETWORKS
          )
 LOOP
  xl.begin_action('Processing ' || r.network);
  xl.begin_action('Setting the Network');
  dwm.set_parameter('NETWORK', r.network);
  xl.end_action(SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK'));

  etl.add_data(
   p_operation => 'INSERT /*+ APPEND PARALLEL(48) */',
   p_tgt => 'FACT_PATIENT_METRIC_DIAG',
   p_src => 'V_FACT_PATIENT_METRIC_DIAG',
   p_commit_at => -1);

  xl.end_action;
 END LOOP;

 xl.close_log('Successfully completed');
EXCEPTION
 WHEN OTHERS THEN
  ROLLBACK;
  xl.close_log(SQLERRM, TRUE);

  RAISE;
END;



END pkg_refresh_cdw_star_schema;
/