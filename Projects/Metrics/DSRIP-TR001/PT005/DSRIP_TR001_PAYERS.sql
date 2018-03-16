DROP TABLE dsrip_tr001_payers PURGE;

CREATE TABLE dsrip_tr001_payers
(
  network     VARCHAR2(3 BYTE),
  visit_id    NUMBER(12),
  payer_id    NUMBER(12),
  payer_rank  NUMBER, 
  CONSTRAINT pk_tr001_payers PRIMARY KEY(network, visit_id, payer_id)
) ORGANIZATION INDEX;

GRANT SELECT ON dsrip_tr001_payers TO PUBLIC;
 