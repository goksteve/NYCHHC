DECLARE
 v_table   VARCHAR2(100) := 'FACT_RESULTS';
BEGIN

 EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table || ' RENAME TO ' || v_table || '_STG1';

 EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table || '_STG  ' || 'RENAME TO ' || v_table;

 EXECUTE IMMEDIATE 'ALTER TABLE ' || v_table || '_STG1 ' || ' RENAME TO ' || v_table || '_STG';
EXCEPTION
 WHEN OTHERS THEN
  NULL;
END;