drop table REF_PRACTICE_GROUP cascade constraints;
CREATE TABLE ref_practice_group
(
 faclity_key     NUMBER(12) NOT NULL,
 facility_name    VARCHAR2(55 BYTE) NOT NULL,
 practice_name   VARCHAR2(55 BYTE) NOT NULL
);

CREATE UNIQUE INDEX pk_ref_practice_group
 ON ref_practice_group( faclity_key, practice_name);

ALTER TABLE ref_practice_group ADD (
  CONSTRAINT pk_ref_practice_group
  PRIMARY KEY
  ( faclity_key, practice_name)
  USING INDEX pk_ref_practice_group
  ENABLE VALIDATE);

GRANT SELECT ON ref_practice_group TO PUBLIC;

INSERT INTO
 ref_practice_group
VALUES
 (27, 'Bellevue', 'BE Cardiology');

INSERT INTO
 ref_practice_group
VALUES
 (21, 'Coney Island', 'CI Cardiology    ');

INSERT INTO
 ref_practice_group
VALUES
 (43, 'Gouverneur', 'GV Cardiology   ');

INSERT INTO
 ref_practice_group
VALUES
 (4, 'Jacobi', 'JA Cardiology    ');

INSERT INTO
 ref_practice_group
VALUES
 (16, 'Kings County', 'KC Cardiology   ');

INSERT INTO
 ref_practice_group
VALUES
 (11, 'Lincoln', 'LI Cardiology   ');

INSERT INTO
 ref_practice_group
VALUES
 (10, 'Metropolitan', 'ME Cardiology   ');

INSERT INTO
 ref_practice_group
VALUES
 (5, 'NCB   (North  Central Bronx)', 'NO Cardiology   ');

INSERT INTO
 ref_practice_group
VALUES
 (7, 'Woodhull', 'WO Cardio     ');
commit;