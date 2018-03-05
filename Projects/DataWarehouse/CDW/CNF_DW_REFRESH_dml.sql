begin
  xl.open_log('TST-OK', 'Re-setting MAX CIDs', true);

  for r in
  (
    select distinct schema_name, table_name
    from log_incremental_data_load
    where table_name not in ('EVENT','PROC_EVENT','RESULT','PROC_EVENT_ARCHIVE')
  )
  loop
    etl.add_data
    (
      p_operation => 'MERGE /*+ parallel(16) */',
      p_tgt => 'LOG_INCREMENTAL_DATA_LOAD',
      p_src => 'SELECT '''||r.schema_name||''' schema_name, '''||r.table_name||''' table_name, network, MAX(cid) max_cid FROM '||r.table_name||' GROUP BY network',
      p_commit_at => -1
    );
  end loop;
  
  xl.close_log('Successfully completed');
exception
 when others then
  rollback;
  xl.close_log(sqlerrm, true);
  raise;
end;
/
