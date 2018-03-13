exec dbm.drop_tables('FACT_VISITS,ERR_FACT_VISITS');

CREATE TABLE fact_visits
(
  network	                  CHAR(3 BYTE) NOT NULL,
  visit_id	                NUMBER(12) NOT NULL,
  patient_key	              NUMBER(12) NOT NULL,
  patient_id                NUMBER(12) NOT NULL,
  facility_key	            NUMBER(12) NOT NULL,
  first_department_key	    NUMBER(12),
  last_department_key	      NUMBER(12),
  attending_provider_key	  NUMBER(12),
  resident_provider_key	    NUMBER(12),
  admitting_provider_key	  NUMBER(12),
  visit_emp_provider_key	  NUMBER(12),
  admission_dt	            DATE NOT NULL,
  admission_dtnum           NUMBER(8),
  patient_age_at_admission  NUMBER(3),
  discharge_dt	            DATE,
  visit_number	            VARCHAR2(50 BYTE),
  initial_visit_type_id	    NUMBER(12),
  final_visit_type_id	      NUMBER(12),
  visit_status_id	          NUMBER(12),
  visit_activation_time	    DATE,
  discharge_type_key	      NUMBER(12),
  financial_class_id	      NUMBER(12),
  physician_service_id	    VARCHAR2(12 BYTE),
  first_payer_key           NUMBER(12),
  source	                  VARCHAR2(64 BYTE) DEFAULT 'QCPR',
  load_date	                DATE DEFAULT SYSDATE,
  loaded_by                 VARCHAR2(30) DEFAULT SYS_CONTEXT('USERENV','OS_USER'),
  cid                       NUMBER(14)
)
COMPRESS BASIC
PARTITION BY LIST(network)
SUBPARTITION BY HASH(visit_id) SUBPARTITIONS 16 
(
  PARTITION cbn VALUES('CBN'),
  PARTITION gp1 VALUES('GP1'),
  PARTITION gp2 VALUES('GP2'),
  PARTITION nbn VALUES('NBN'),
  PARTITION nbx VALUES('NBX'),
  PARTITION qhn VALUES('QHN'),
  PARTITION sbn VALUES('SBN'),
  PARTITION smn VALUES('SMN')
);

GRANT SELECT ON fact_visits TO PUBLIC;

EXEC dbms_errlog.create_error_log('FACT_VISITS','ERR_FACT_VISITS');
ALTER TABLE err_fact_visits ADD entry_ts TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL;

CREATE UNIQUE INDEX pk_fact_visits ON fact_visits(visit_id, network) LOCAL PARALLEL 16;
ALTER INDEX pk_fact_visits NOPARALLEL;

ALTER TABLE fact_visits ADD CONSTRAINT pk_fact_visits PRIMARY KEY(visit_id, network) USING INDEX pk_fact_visits;

CREATE OR REPLACE TRIGGER tr_insert_fact_visits
FOR INSERT OR UPDATE ON fact_visits
COMPOUND TRIGGER
  BEFORE STATEMENT IS
  BEGIN
    dwm.init_max_cids('FACT_VISITS');
  END BEFORE STATEMENT;

  AFTER EACH ROW IS
  BEGIN
    dwm.max_cids(:new.network) := GREATEST(dwm.max_cids(:new.network), :new.cid);
  END AFTER EACH ROW;

  AFTER STATEMENT IS
  BEGIN
    dwm.record_max_cids('FACT_VISITS');
  END AFTER STATEMENT;
END tr_insert_fact_visits;
/
