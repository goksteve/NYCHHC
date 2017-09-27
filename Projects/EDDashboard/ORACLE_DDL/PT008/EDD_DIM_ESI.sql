exec dbm.drop_tables('EDD_DIM_ESI');

CREATE TABLE edd_dim_esi
(
  ESIKey  NUMBER(10,0) CONSTRAINT pk_edd_dim_esi PRIMARY KEY,
  ESI     VARCHAR2(1000)
) ORGANIZATION INDEX;

GRANT SELECT ON edd_dim_esi TO PUBLIC;

INSERT INTO EDD_DIM_ESI VALUES(-1, 'Unknown');
INSERT INTO EDD_DIM_ESI VALUES(0, 'Any');
INSERT INTO EDD_DIM_ESI VALUES(1, 'Immediate');
INSERT INTO EDD_DIM_ESI VALUES(2, 'Emergent');
INSERT INTO EDD_DIM_ESI VALUES(3, 'Urgent');
INSERT INTO EDD_DIM_ESI VALUES(4, 'Semi-Urgent');
INSERT INTO EDD_DIM_ESI VALUES(5, 'Non-Urgent');

COMMIT;