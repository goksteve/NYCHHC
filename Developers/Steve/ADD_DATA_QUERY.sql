ALTER SESSION ENABLE parallel DML;
--*****************************************
--begin
--  xl.open_log(' steve FACT_PATIENT_PRESCRIPTIONS',' steve V_FACT_PATIENT_PRESCRIPTIONS', TRUE);
--  
--  etl.add_data
--  (
--    p_operation => 'REPLACE /*+ Parallel(32)  */',
--    p_tgt => 'DIM_PATIENTS1',
--    p_src => 'V_FACT_PATIENT_PRESCRIPTIONS',
--   -- p_whr => 'Where 1= 1 ',
--    p_commit_at => -1
--  ); 
--  
--  xl.close_log('COMPLETE OK ');
--EXCEPTION
-- when others THEN
--  xl.close_log(SQLERRM, TRUE );
--  RAISE ;
--end;
--**********************************************
--*******FACT_VISITS *****************************
--EXECUTE dwm.refresh_data('where etl_step_num = 1101')  
--EXECUTE dwm.refresh_data('where etl_step_num = 4110')  

--**********************************************
select * from CNF_DW_REFRESH
--where target_table  = 'FACT_RESULTS'
order by etl_step_num desc;
--**********************************************
--EXECUTE dwm.refresh_data('where etl_step_num >= 3110 and etl_step_num <= 3160');


--**********************************************
select * from LOG_INCREMENTAL_DATA_LOAD;
--**********************************************

select  * from  DBG_LOG_DATA where action like '%steve%';
--**************************************************************
select a.*, DBMS_LOB.substr(comment_txt, 250) from DBG_LOG_DATA a
where
tstamp > date '2018-08-15'
and proc_id   > 886
order by proc_id DESC ,tstamp desc;
--************************************************************

select a.*, DBMS_LOB.substr(comment_txt, 250) from DBG_LOG_DATA a
where  upper( action) like '%DAILY%'
order by tstamp desc;

--**************************************
INSERT INTO cnf_dw_refresh  VALUES    (2120, 'REPLACE /*+ APPEND PARALLEL(32) */', 'FACT_VISIT_PAYERS', 'V_FACT_VISIT_PAYERS', NULL,     NULL, NULL, NULL);
COMMIT;
--*******************************

--****************************************************************************************************
--************PACKAGE REFRESH STAR SCHEMA ************
BEGIN
rfs.sp_refresh_ref_tables();
-- dwm.refresh_data('where etl_step_num = 710');
-- dwm.refresh_data('where etl_step_num = 720');
-- dwm.refresh_data('where etl_step_num = 730');
-- dwm.refresh_data('where etl_step_num = 740');
-- dwm.refresh_data('where etl_step_num = 750');
-- dwm.refresh_data('where etl_step_num = 760');
-- dwm.refresh_data('where etl_step_num = 770');
END;
---*********************************************
BEGIN
 rfs.sp_refresh_dim_tables(); ---  BETWEEN 820 AND 1020 ---
END;



CREATE BITMAP INDEX bidx_result_history_dtelem_id ON result_history_cdw(data_element_id, network) local parallel 32;
CREATE INDEX idx_result_history_cdw_cid  ON result_history_cdw(cid, network) LOCAL parallel 32 ;
CREATE UNIQUE INDEX pk_result_history_cdw ON result_history_cdw(  visit_id,  data_element_id,  event_id,  result_history_cdw_report_number,  multi_field_occurrence_number,  item_number,  network) local parallel 32;
ALTER TABLE result_history_cdw ADD (   CONSTRAINT pk_result_history_cdw  PRIMARY KEY  (network, visit_id, event_id, data_element_id, result_history_cdw_report_number, multi_field_occurrence_number, item_number)  USING INDEX LOCAL 
  ENABLE VALIDATE);