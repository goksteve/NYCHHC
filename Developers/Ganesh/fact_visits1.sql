exec dbm.drop_tables('fact_visits1,ERR_fact_visits1');

CREATE TABLE fact_visits1
(
  network	                  CHAR(3 BYTE) NOT NULL,
  visit_id	                NUMBER(12) NOT NULL,
  patient_key	              NUMBER(12) NOT NULL,
  patient_id                NUMBER(12) NOT NULL,
  facility_key	            NUMBER(12) NOT NULL,
  admission_dt_key          NUMBER(8) NOT NULL,
  admission_dt	            DATE NOT NULL,
  discharge_dt_key          NUMBER(8),
  discharge_dt	            DATE,
  first_department_key	    NUMBER(12),
  last_department_key	      NUMBER(12),
  attending_provider_key	  NUMBER(12),
  resident_provider_key	    NUMBER(12),
  admitting_provider_key	  NUMBER(12),
  visit_emp_provider_key	  NUMBER(12),
  discharge_type_key	      NUMBER(12),
  first_payer_key           NUMBER(12),
  patient_age_at_admission  NUMBER(3),
  visit_number	            VARCHAR2(50 BYTE),
  initial_visit_type_id	    NUMBER(12),
  final_visit_type_id	      NUMBER(12),
  visit_status_id	          NUMBER(12),
  visit_activation_time	    DATE,
  financial_class_id	      NUMBER(12),
  physician_service_id	    VARCHAR2(12 BYTE),
  source	                  VARCHAR2(64 BYTE) DEFAULT 'QCPR' NOT NULL,
  load_dt	                  DATE DEFAULT SYSDATE NOT NULL,
  loaded_by                 VARCHAR2(30) DEFAULT SYS_CONTEXT('USERENV','OS_USER') NOT NULL,
  cid                       NUMBER(14) NOT NULL
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

GRANT SELECT ON fact_visits1 TO PUBLIC;

EXEC dbms_errlog.create_error_log('fact_visits1','ERR_fact_visits1');
ALTER TABLE err_fact_visits1 ADD entry_ts TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL;

CREATE UNIQUE INDEX pk_fact_visits1 ON fact_visits1(visit_id, network) LOCAL PARALLEL 32;
ALTER INDEX pk_fact_visits1 NOPARALLEL;

ALTER TABLE fact_visits1 ADD CONSTRAINT pk_fact_visits1 PRIMARY KEY(visit_id, network) USING INDEX pk_fact_visits1;

CREATE INDEX idx_fact_visits1_adm_dtkey ON fact_visits1(admission_dt_key) LOCAL PARALLEL 32;
ALTER INDEX idx_fact_visits1_adm_dtkey NOPARALLEL;
