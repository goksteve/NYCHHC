EXEC dbm.drop_tables('ERROR_FACT_VISIT_RSLT_METRIC')

CREATE TABLE error_fact_visit_rslt_metric
(
 network          CHAR(3 CHAR) NULL,
 original_valie   VARCHAR2(2048 CHAR) NULL,
 error_value      VARCHAR2(1024 CHAR) NULL,
 err_date         DATE DEFAULT SYSDATE NULL
)
LOGGING
NOCOMPRESS
NOCACHE
MONITORING;

GRANT SELECT ON error_fact_visit_rslt_metric TO PUBLIC;
/