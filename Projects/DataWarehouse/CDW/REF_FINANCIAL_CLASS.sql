exec dbm.drop_tables('REF_FINANCIAL_CLASS');

CREATE TABLE ref_financial_class
(
  network                CHAR(3 BYTE) NOT NULL,
  financial_class_id     NUMBER(12) NOT NULL,
  financial_class_name                   VARCHAR2(100 BYTE),
  CONSTRAINT pk_ref_financial_class PRIMARY KEY(network, financial_class_id)
) COMPRESS BASIC;

GRANT SELECT ON ref_financial_class TO PUBLIC;

