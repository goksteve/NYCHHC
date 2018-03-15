CREATE TABLE map_visit_sec_nbr_type
(
  network                 CHAR(3 BYTE),
  facility_id             NUMBER(2),
  visit_sec_nbr_type_id   NUMBER(12) NOT NULL,
  CONSTRAINT pk_map_secondary_num_type PRIMARY KEY(network, facility_id)
)
ORGANIZATION INDEX;

INSERT INTO map_visit_sec_nbr_type VALUES('CBN', 4, 22);
INSERT INTO map_visit_sec_nbr_type VALUES('CBN', 5, 21);
INSERT INTO map_visit_sec_nbr_type VALUES('GP1', 1, 18);
INSERT INTO map_visit_sec_nbr_type VALUES('GP1', 2, 12);
INSERT INTO map_visit_sec_nbr_type VALUES('GP1', 3, 14);
INSERT INTO map_visit_sec_nbr_type VALUES('GP2', 2,  4);
INSERT INTO map_visit_sec_nbr_type VALUES('NBN', 1,  9);
INSERT INTO map_visit_sec_nbr_type VALUES('NBN', 2,  9);
INSERT INTO map_visit_sec_nbr_type VALUES('NBX', 2, 13);
INSERT INTO map_visit_sec_nbr_type VALUES('QHN', 2, 13);
INSERT INTO map_visit_sec_nbr_type VALUES('SBN', 1, 11);
INSERT INTO map_visit_sec_nbr_type VALUES('SMN', 1, 15);
INSERT INTO map_visit_sec_nbr_type VALUES('SMN', 2, 12);
INSERT INTO map_visit_sec_nbr_type VALUES('SMN', 7, 17);
INSERT INTO map_visit_sec_nbr_type VALUES('SMN', 8, 18);
INSERT INTO map_visit_sec_nbr_type VALUES('SMN', 9, 24);

commit;
