CREATE OR REPLACE FUNCTION CDW.fn_str_to_number(net IN VARCHAR2, orig_str IN VARCHAR2, str IN VARCHAR2)
 RETURN NUMBER AS
 rtn   NUMBER;
 PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN

 BEGIN
  rtn := TO_NUMBER(str);
 EXCEPTION
  WHEN OTHERS THEN

   INSERT INTO
    error_fact_visit_rslt_metric(network, original_valie, error_value)
   VALUES
    (net, orig_str, str);
   COMMIT;
   rtn := -1;
 END;

 RETURN rtn;
END;
/


