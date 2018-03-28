ALTER SESSION ENABLE parallel DML;

begin
  xl.open_log(' steve FACT_PATIENT_PRESCRIPTIONS',' steve V_FACT_PATIENT_PRESCRIPTIONS', TRUE);
  
  etl.add_data
  (
    p_operation => 'REPLACE /*+ Parallel(32)  */',
    p_tgt => 'FACT_PATIENT_PRESCRIPTIONS',
    p_src => 'V_FACT_PATIENT_PRESCRIPTIONS',
   -- p_whr => 'Where 1= 1 ',
    p_commit_at => -1
  ); 
  
  xl.close_log('COMPLETE OK ');
EXCEPTION
 when others THEN
  xl.close_log(SQLERRM, TRUE );
  RAISE ;
end;

select  * from  DBG_LOG_DATA where action like '%steve%'
