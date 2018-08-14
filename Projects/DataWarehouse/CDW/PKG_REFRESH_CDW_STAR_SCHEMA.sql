CREATE OR REPLACE PACKAGE pkg_refresh_cdw_star_schema AS
 /*
   Package PKG_REFRESH_CDW_STAR_SCHEMA contains stored procedures for performing data refresh of REF, STG, DIM and FACT Tables:
   History of changes (newest to oldest):
   ------------------------------------------------------------------------------
   09-MAY-2018, SG: created package
 */
 -- Procedure SP_START_REFRESH to start refresh process, in the order of REF, DIM, FACT and METRIC.
 PROCEDURE sp_start_refresh /*(p_refresh_type IN VARCHAR2, p_refresh_step VARCHAR2)*/ ;

 -- Procedure SP_REFRESH_REF_TABLES to refresh only reference tables
 PROCEDURE sp_refresh_ref_tables;

 -- Procedure SP_REFRESH_DIM_TABLES to refresh only dimension tables
 PROCEDURE sp_refresh_dim_tables;

 -- Procedure SP_REFRESH_RX_FACT TABLES to refresh only prescription fact tables
 PROCEDURE sp_refresh_rx_fact_tables;

 -- Procedure SP_REFRESH_FACT_VISITS_FULL to refresh only fact-visits tables
 PROCEDURE sp_refresh_fact_visits_full;

 -- Procedure SP_REFRESH_FACT_TABLES to refresh the fact tables other than fact_visits, fact_prescription and fact_results
 PROCEDURE sp_refresh_fact_tables;

 -- Procedure SP_REFRESH_FACT_RESULTS_FULL to refresh only fact-results tables
 PROCEDURE sp_refresh_fact_results_full;

 -- Procedure SP_REFRESH_METRIC_TABLES to refresh the fact-metric tables(Compass)
 PROCEDURE sp_ref_fact_visit_metric_rslt;

 -- Procedure SP_REFRESH_PATIENT_METRIC_DIAG to refresh the fact-metric tables(Compass)
 PROCEDURE sp_refresh_patient_metric_diag;

 -- Procedure  SP_FACT_VISIT_METRIC_FLAGS to refresh the fact-metric tables(Compass)
 PROCEDURE sp_fact_visit_metric_flags;

--**************************************************************
END pkg_refresh_cdw_star_schema;
/
CREATE OR REPLACE PUBLIC SYNONYM RFS FOR PKG_REFRESH_CDW_STAR_SCHEMA;

CREATE OR REPLACE PACKAGE BODY pkg_refresh_cdw_star_schema AS
 /******************************************************************************
    NAME:       PKG_REFRESH_CDW_STAR_SCHEMA
    PURPOSE:

    REVISIONS:
    Ver        Date        Author           Description
    ---------  ----------  ---------------  ------------------------------------
    1.0        05/10/2018      goreliks1       1. Created this package body.
 ******************************************************************************/

 PROCEDURE sp_start_refresh /*(p_refresh_type IN VARCHAR2, p_refresh_step VARCHAR2)*/
                           IS
 -- *******  DESCRIPTION ******************************
 -- p_refresh_type  => 'F' - full  / 'I' - incremental
 -- p_refresh_step => 'ALL', 'REF','DIM', 'FACT', 'METRIC'
 --*****************************************************
 --  v_type   VARCHAR2(2) := p_refresh_type;
 --  v_step   VARCHAR2(10) := p_refresh_type;
 BEGIN
  --  IF v_type = 'F' THEN
  --   IF v_step = 'ALL' THEN
  rfs.sp_refresh_ref_tables;
  rfs.sp_refresh_dim_tables;
  sp_refresh_rx_fact_tables;
  rfs.sp_refresh_fact_visits_full;
  rfs.sp_refresh_fact_tables;
  rfs.sp_refresh_fact_results_full;

 --   END IF;
 --  END IF;
 END;

 PROCEDURE sp_refresh_ref_tables IS
 BEGIN
  dwm.refresh_data('where etl_step_num >=710 and etl_step_num <=770');
 END;

 PROCEDURE sp_refresh_dim_tables IS
 BEGIN
  dwm.refresh_data('where etl_step_num >=820 and etl_step_num <=1020');
 END;

 PROCEDURE sp_refresh_rx_fact_tables IS
 BEGIN
  dwm.refresh_data('where etl_step_num >=1101 and etl_step_num <=1106');
 END;

 PROCEDURE sp_refresh_fact_visits_full IS
 --  v_table   VARCHAR2(100) := 'FACT_VISITS';
 --  n_cnt     NUMBER;
 BEGIN
  xl.
   open_log(
   'FACT_VISITS - RESET MAX CID NUMBERS AND DROP INDEXES',
   SYS_CONTEXT('USERENV', 'OS_USER') || ': reset max cid numbers and drop indexes for FACT_VISITS',
   TRUE);

  UPDATE
   log_incremental_data_load
  SET
   max_cid = 0
  WHERE
   table_name = 'FACT_VISITS';
  COMMIT;

  --************   DROP CONSTRAINTS/ INDEXES /TRIGGERES **************
  BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE FACT_VISITS DROP CONSTRAINT PK_FACT_VISITS';
  EXCEPTION
   WHEN OTHERS THEN
    NULL;
  END;

  BEGIN
   EXECUTE IMMEDIATE 'DROP INDEX PK_FACT_VISITS';
   EXECUTE IMMEDIATE 'DROP INDEX IDX_FACT_VISIT_ADMISSION';
   EXECUTE IMMEDIATE 'DROP INDEX IDX_FACT_VISIT_ADM_DTKEY';
   EXECUTE IMMEDIATE 'DROP INDEX UI_FACT_VISITS';
  EXCEPTION
   WHEN OTHERS THEN
    NULL;
  END;

  BEGIN
   EXECUTE IMMEDIATE 'DROP TRIGGER TR_INSERT_FACT_VISITS';
  EXCEPTION
   WHEN OTHERS THEN
    NULL;
  END;

  xl.close_log('Successfully completed');
  --*************  LOAD TABLE  ***************************
  -- IF n_cnt > 0 THEN
  dwm.refresh_data('where etl_step_num = 2000');
  --******************************************************

  xl.open_log('Create Indexes/RTriggers For FACT_VISITS', 'Create Indexes For FACT_VISITS', TRUE);

  ---  CREATE CONSTRAINTS, INDEXES AND TRIGGERS BACK ----------
  EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX PK_FACT_VISITS ON FACT_VISITS(visit_key) PARALLEL 32 ';
  EXECUTE IMMEDIATE 'ALTER INDEX PK_FACT_VISITS  NOPARALLEL ';
  EXECUTE IMMEDIATE
   'ALTER TABLE FACT_VISITS ADD CONSTRAINT PK_FACT_VISITS PRIMARY KEY(visit_key) USING INDEX PK_FACT_VISITS';

  EXECUTE IMMEDIATE 'CREATE UNIQUE INDEX UI_FACT_VISITS  ON FACT_VISITS(VISIT_ID, NETWORK) LOCAL PARALLEL 32';
  EXECUTE IMMEDIATE 'ALTER INDEX UI_FACT_VISITS NOPARALLEL';

  EXECUTE IMMEDIATE 'CREATE INDEX IDX_FACT_VISIT_ADM_DTKEY ON FACT_VISITS(ADMISSION_DT_KEY) PARALLEL 32';
  EXECUTE IMMEDIATE 'ALTER INDEX IDX_FACT_VISIT_ADM_DTKEY NOPARALLEL';

  EXECUTE IMMEDIATE 'CREATE INDEX IDX_FACT_VISIT_ADMISSION  ON FACT_VISITS(ADMISSION_DT) PARALLEL 32';
  EXECUTE IMMEDIATE 'ALTER INDEX IDX_FACT_VISIT_ADMISSION  NOPARALLEL';

  -- Tirgger
  EXECUTE IMMEDIATE
      'CREATE OR REPLACE TRIGGER tr_insert_FACT_VISITS '
   || 'FOR INSERT OR UPDATE '
   || 'ON FACT_VISITS  '
   || 'COMPOUND TRIGGER  '
   || 'BEFORE STATEMENT IS '
   || 'BEGIN '
   || ' dwm.init_max_cids(''FACT_VISITS''); '
   || 'END BEFORE STATEMENT;  '
   || 'AFTER EACH ROW IS '
   || 'BEGIN '
   || '  dwm.max_cids(:new.network) := GREATEST(dwm.max_cids(:new.network), :new.cid); '
   || 'END AFTER EACH ROW; '
   || ' AFTER STATEMENT IS '
   || ' BEGIN '
   || ' dwm.record_max_cids(''FACT_VISITS''); '
   || 'END AFTER STATEMENT; '
   || ' END tr_insert_FACT_VISITS; ';

  ---- update  log_incremental_data_load table
  xl.begin_action('Updateing log_incremental_data_load table with CID for FACT_VISITS');

  BEGIN
   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_visits PARTITION(cbn)
     )
   WHERE
    table_name = 'FACT_VISITS' AND network = 'CBN';

   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_visits PARTITION(gp1)
     )
   WHERE
    table_name = 'FACT_VISITS' AND network = 'GP1';

   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_visits PARTITION(gp2)
     )
   WHERE
    table_name = 'FACT_VISITS' AND network = 'GP2';

   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_visits PARTITION(nbn)
     )
   WHERE
    table_name = 'FACT_VISITS' AND network = 'NBN';

   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_visits PARTITION(nbx)
     )
   WHERE
    table_name = 'FACT_VISITS' AND network = 'NBX';

   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_visits PARTITION(qhn)
     )
   WHERE
    table_name = 'FACT_VISITS' AND network = 'QHN';

   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_visits PARTITION(sbn)
     )
   WHERE
    table_name = 'FACT_VISITS' AND network = 'SBN';

   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_visits PARTITION(smn)
     )
   WHERE
    table_name = 'FACT_VISITS' AND network = 'SMN';

  END;

  xl.end_action;
  COMMIT;
  xl.close_log('Successfully completed');
 EXCEPTION
  WHEN OTHERS THEN
   ROLLBACK;
   xl.close_log(SQLERRM, TRUE);
 END;

 PROCEDURE sp_refresh_fact_tables IS
 BEGIN

  dwm.refresh_data('where etl_step_num >=2020 and etl_step_num <=2130');
 END;
--***************** SP_REFRESH_FACT_RESULTS_FULL ************************

 PROCEDURE sp_refresh_fact_results_full IS
 --  v_table   VARCHAR2(100) := 'FACT_RESULTS';
 --  n_cnt     NUMBER;
 BEGIN

  xl.
   open_log(
   'FACT_RESULTS - RESET MAX CID NUMBERS AND DROP INDEXES',
   SYS_CONTEXT('USERENV', 'OS_USER') || ': reset max cid numbers and drop indexes for FACT_RESULTS',
   TRUE);

  UPDATE
   log_incremental_data_load
  SET
   max_cid = 0
  WHERE
   table_name = 'FACT_RESULTS';
  COMMIT;

  --************   DROP INDEXES /TRIGGERES **************
  BEGIN
   EXECUTE IMMEDIATE 'ALTER TABLE FACT_RESULTS DROP CONSTRAINT PK_FACT_RESULTS';
   EXECUTE IMMEDIATE 'DROP INDEX PK_FACT_RESULTS';
   EXECUTE IMMEDIATE 'DROP INDEX IDX_FACT_RESULTS_VST_KEY';
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

  xl.close_log('Successfully completed');
  --*************  LOAD TABLE  ***************************
  -- IF n_cnt > 0 THEN
  dwm.refresh_data('where etl_step_num = 3110');
  dwm.refresh_data('where etl_step_num = 3120');
  dwm.refresh_data('where etl_step_num = 3130');
  dwm.refresh_data('where etl_step_num = 3140');
  dwm.refresh_data('where etl_step_num = 3150');
  dwm.refresh_data('where etl_step_num = 3160');

 --*****INSERTING DATA FROM PROC_EVENT_TABLE ********
  dwm.refresh_data('where etl_step_num = 3165') ;
  --******************************************************

  xl.open_log('Create Indexes/RTriggers For Fact_Results', 'Create Indexes For Fact_Results', TRUE);

  ---  CREATE INDEXES AND TRIGGERS BACK ----------
  -- Indexes ---
  EXECUTE IMMEDIATE
   'CREATE UNIQUE INDEX pk_fact_results  ON fact_results( visit_id,event_id,data_element_id,result_report_number,multi_field_occurrence_number,item_number,network) PARALLEL 32 ';
  EXECUTE IMMEDIATE 'ALTER INDEX pk_fact_results  NOPARALLEL ';
  EXECUTE IMMEDIATE
   'ALTER TABLE fact_results ADD CONSTRAINT pk_fact_results PRIMARY KEY(visit_id, event_id, data_element_id, result_report_number, multi_field_occurrence_number, item_number,network) USING INDEX pk_fact_results';
  EXECUTE IMMEDIATE 'CREATE INDEX IDX_FACT_RESULTS_VST_KEY ON FACT_RESULTS(VISIT_KEY) PARALLEL 32';
  EXECUTE IMMEDIATE 'ALTER INDEX UI_FACT_RESULTS_KEY NOPARALLEL';
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
  xl.begin_action('Updateing log_incremental_data_load table with CID for fact_results');

  BEGIN
   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_results PARTITION(cbn)
     )
   WHERE
    table_name = 'FACT_RESULTS' AND network = 'CBN';

   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_results PARTITION(gp1)
     )
   WHERE
    table_name = 'FACT_RESULTS' AND network = 'GP1';
   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_results PARTITION(gp2)
     )
   WHERE
    table_name = 'FACT_RESULTS' AND network = 'GP2';
   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_results PARTITION(nbn)
     )
   WHERE
    table_name = 'FACT_RESULTS' AND network = 'NBN';
   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_results PARTITION(nbx)
     )
   WHERE
    table_name = 'FACT_RESULTS' AND network = 'NBX';
   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_results PARTITION(qhn)
     )
   WHERE
    table_name = 'FACT_RESULTS' AND network = 'QHN';
   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_results PARTITION(sbn)
     )
   WHERE
    table_name = 'FACT_RESULTS' AND network = 'SBN';
   UPDATE /*+ PARALLEL(32) */
    log_incremental_data_load
   SET
    max_cid =
     (
      SELECT
       MAX(cid) AS max_cid
      FROM
       fact_results PARTITION(smn)
     )
   WHERE
    table_name = 'FACT_RESULTS' AND network = 'SMN';
  END;

  xl.end_action;
  COMMIT;
  xl.close_log('Successfully completed');
 EXCEPTION
  WHEN OTHERS THEN
   ROLLBACK;
   xl.close_log(SQLERRM, TRUE);
 END;
--*************** SP_REF_FACT_VISIT_METRIC_RSLT ****************
 PROCEDURE sp_ref_fact_visit_metric_rslt IS
 BEGIN
  xl.open_log('FACT_VISIT_METRIC_RESULTS', 'Refreshing FACT_VISIT_METRIC_RESULTS', TRUE);

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
             dim_hc_networks
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
--***************************************************************
--*************** SP_REFRESH_PATIENT_METRIC_DIAG ****************
 PROCEDURE sp_refresh_patient_metric_diag AS
 BEGIN
  xl.open_log('sp_refresh_patient_metric_diag', 'Refreshing FACT_PATIENT_METRIC_DIAG', TRUE);

  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
  EXECUTE IMMEDIATE 'TRUNCATE TABLE FACT_PATIENT_METRIC_DIAG';
  --
  --  FOR r IN (
  --            SELECT
  --             DISTINCT network
  --            FROM
  --             dim_hc_networks
  --           )
  --  LOOP
  --   xl.begin_action('Processing ' || r.network);
  --   xl.begin_action('Setting the Network');
  --   dwm.set_parameter('NETWORK', r.network);
  --   xl.end_action(SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK'));

  etl.add_data(
   p_operation => 'INSERT /*+ APPEND PARALLEL(48) */',
   p_tgt => 'FACT_PATIENT_METRIC_DIAG',
   p_src => 'V_FACT_PATIENT_METRIC_DIAG',
   p_commit_at => -1);

  xl.end_action;
  --  END LOOP;

  xl.close_log('Successfully completed');
 EXCEPTION
  WHEN OTHERS THEN
   ROLLBACK;
   xl.close_log(SQLERRM, TRUE);

   RAISE;
 END;
--************************  SP_FACT_VISIT_METRIC_FLAGS ***************
 PROCEDURE sp_fact_visit_metric_flags AS
 BEGIN
  xl.open_log('SP_FACT_VISIT_METRIC_FLAGS', 'Refreshing FACT_VISIT_METRIC_FLAGS', TRUE);

  EXECUTE IMMEDIATE 'TRUNCATE TABLE FACT_VISIT_METRIC_FLAGS';
  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DDL';
  EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

  FOR r IN (
            SELECT
             network
            FROM
             dim_hc_networks
           )
  LOOP
   xl.begin_action('Processing ' || r.network);

   xl.begin_action('Setting the Network');
   dwm.set_parameter('NETWORK', r.network);
   xl.end_action(SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK'));

   etl.add_data(
    p_operation => 'INSERT /*+ APPEND PARALLEL(48) */',
    p_tgt => 'FACT_VISIT_METRIC_FLAGS',
    p_src => 'V_FACT_VISIT_METRIC_FLAGS',
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


CREATE OR REPLACE PUBLIC SYNONYM RFS FOR PKG_REFRESH_CDW_STAR_SCHEMA;
