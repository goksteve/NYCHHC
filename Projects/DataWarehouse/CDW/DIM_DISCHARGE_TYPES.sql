--CREATE SEQUENCE seq_dim_discharge_types START WITH 1;

EXEC dbm.drop_tables('DIM_DISCHARGE_TYPES');

CREATE TABLE dim_discharge_types
(
 discharge_type_key   NUMBER(12) NOT NULL,
 network              CHAR(3 BYTE) NULL,
 visit_type_id        NUMBER(12) NOT NULL,
 discharge_type_id    NUMBER(12) NOT NULL,
 discharge_type_name                VARCHAR2(100 BYTE) NULL,
 facility_id          NUMBER(12) NULL,
 load_dt              DATE DEFAULT SYSDATE NOT NULL,
 loaded_by            VARCHAR2(30 BYTE) DEFAULT SYS_CONTEXT('USERENV', 'OS_USER') NULL
)
COMPRESS BASIC;

CREATE UNIQUE INDEX pk_dim_discharge_types
 ON dim_discharge_types(discharge_type_key)
 LOGGING;

ALTER TABLE dim_discharge_types ADD (CONSTRAINT pk_dim_discharge_types PRIMARY KEY(discharge_type_key)USING INDEX pk_dim_discharge_types);

ALTER TABLE dim_discharge_types
 ADD CONSTRAINT uk_dim_discharge_types UNIQUE(network, visit_type_id, discharge_type_id);

GRANT SELECT ON dim_discharge_types TO PUBLIC;

CREATE OR REPLACE TRIGGER bir_dim_discharge_types
BEFORE INSERT ON dim_discharge_types FOR EACH ROW
BEGIN
  IF :new.discharge_type_key IS NULL THEN
    :new.discharge_type_key := seq_dim_discharge_types.NEXTVAL;
  END IF;
END;
/

