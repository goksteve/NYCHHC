select distinct
  column_value table_name,
  'INSERT /*+ parallel(32) */ INTO log_incremental_data_load SELECT network||''DW01'', ''UD_MASTER'', '''||column_value||''', network, MAX(cid), NULL, SYSDATE FROM '||lower(column_value)||' GROUP BY network;
commit;
' cmd 
from table(split_string('EVENT,PROC,PROC_EVENT,PROC_EVENT_ARCHIVE,RESULT,VISIT,VISIT_SEGMENT,VISIT_SEGMENT_VISIT_LOCATION')) cmd
order by 1
;

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

insert into log_incremental_data_load
select 'HIGGSDV3','CDW','FACT_RESULTS',column_value,0,null, sysdate
from table(split_string('CBN,GP1,GP2,NBN,NBX,QHN,SBN,SMN'));
