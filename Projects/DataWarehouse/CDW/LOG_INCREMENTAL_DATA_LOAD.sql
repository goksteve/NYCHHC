exec dbm.drop_tables('LOG_INCREMENTAL_DATA_LOAD');

CREATE TABLE log_incremental_data_load
(
  dbname        CHAR(7 BYTE) as (network||'DW01'),
  schema_name   VARCHAR2(30 BYTE) DEFAULT 'UD_MASTER' NOT NULL, 
  table_name    VARCHAR2(30 BYTE),
  network       CHAR(3 BYTE) NOT NULL,
  max_cid       NUMBER(14) NOT NULL,
  prev_max_cid  NUMBER(14),
  load_dt       DATE DEFAULT SYSDATE NOT NULL
);

ALTER TABLE log_incremental_data_load ADD CONSTRAINT pk_incremental_data_load_log PRIMARY KEY(table_name, network);

CREATE OR REPLACE TRIGGER bur_log_incremental_data_load
BEFORE UPDATE ON log_incremental_data_load FOR EACH ROW
BEGIN
  IF :new.max_cid <> NVL(:old.max_cid, 0) THEN
    :new.prev_max_cid := :old.max_cid;
    :new.load_dt := SYSDATE;
  END IF;
END;
/

GRANT SELECT ON log_incremental_data_load TO PUBLIC;
