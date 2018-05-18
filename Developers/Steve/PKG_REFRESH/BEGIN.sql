BEGIN

 EXECUTE IMMEDIATE
     'CREATE OR REPLACE TRIGGER tr_insert_fact_results_stg '
  || 'FOR INSERT OR UPDATE '
  || 'ON fact_results_stg  '
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
  || ' END tr_insert_fact_results_stg; ';

END;