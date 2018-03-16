truncate table log_incremental_data_load;

INSERT /*+ parallel(32) */ INTO log_incremental_data_load SELECT network||'DW01', 'UD_MASTER', 'EVENT', network, MAX(cid), NULL, SYSDATE FROM event GROUP BY network;
commit;

INSERT /*+ parallel(32) */ INTO log_incremental_data_load SELECT network||'DW01', 'UD_MASTER', 'PROC', network, MAX(cid), NULL, SYSDATE FROM proc GROUP BY network;
commit;

INSERT /*+ parallel(32) */ INTO log_incremental_data_load SELECT network||'DW01', 'UD_MASTER', 'PROC_EVENT', network, MAX(cid), NULL, SYSDATE FROM proc_event GROUP BY network;
commit;

INSERT /*+ parallel(32) */ INTO log_incremental_data_load SELECT network||'DW01', 'UD_MASTER', 'PROC_EVENT_ARCHIVE', network, MAX(cid), NULL, SYSDATE FROM proc_event_archive GROUP BY network;
commit;

INSERT /*+ parallel(32) */ INTO log_incremental_data_load SELECT network||'DW01', 'UD_MASTER', 'RESULT', network, MAX(cid), NULL, SYSDATE FROM result GROUP BY network;
commit;

INSERT /*+ parallel(32) */ INTO log_incremental_data_load SELECT network||'DW01', 'UD_MASTER', 'VISIT', network, MAX(cid), NULL, SYSDATE FROM visit GROUP BY network;
commit;

INSERT /*+ parallel(32) */ INTO log_incremental_data_load SELECT network||'DW01', 'UD_MASTER', 'VISIT_SEGMENT', network, MAX(cid), NULL, SYSDATE FROM visit_segment GROUP BY network;
commit;

INSERT /*+ parallel(32) */ INTO log_incremental_data_load SELECT network||'DW01', 'UD_MASTER', 'VISIT_SEGMENT_VISIT_LOCATION', network, MAX(cid), NULL, SYSDATE FROM visit_segment_visit_location GROUP BY network;
commit;

-- =============================================================================

begin
  xl.open_log('TST_OK', 'Getting Mac CIDs from the FACT_RESULTS table', true);
  
  etl.add_data
  (
    p_operation => 'MERGE /*+ parallel(32) */',
    p_tgt => 'LOG_INCREMENTAL_DATA_LOAD',
    p_src => 'SELECT ''HIGGSDV3'' dbname, ''CDW'' schema_name, ''FACT_RESULTS'' table_name, network, MAX(cid) max_cid' ||
             ' FROM fact_results WHERE network IN ('''','''','''','''') GROUP BY network',
    p_commit_at => -1
  );

  xl.close_log('Successfully completed');
exception
 when others then
  xl.close_log(sqlerrm, true);
  raise;
end;
/
