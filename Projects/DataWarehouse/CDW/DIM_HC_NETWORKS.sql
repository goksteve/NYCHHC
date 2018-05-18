CREATE TABLE dim_hc_networks
(
network_key   NUMBER(12) NOT NULL ,
network       CHAR(3 BYTE) NOT NULL CONSTRAINT dim_hc_networks_pk PRIMARY KEY,
name          VARCHAR2(63 BYTE) NOT NULL
);

CREATE UNIQUE INDEX UI_DIM_HC_NETWORKS ON DIM_HC_NETWORKS
(NETWORK_KEY)
LOGGING;


INSERT INTO dim_hc_networks VALUES (100, 'CBN', 'Central Brooklyn');
INSERT INTO dim_hc_networks VALUES (200, 'GP1', 'Nothern Manhattan Gen 1');
INSERT INTO dim_hc_networks VALUES (300, 'GP2', 'Nothern Manhattan Gen 2');
INSERT INTO dim_hc_networks VALUES (400, 'NBN', 'North Brooklyn');
INSERT INTO dim_hc_networks VALUES (500, 'NBX', 'North Bronx');
INSERT INTO dim_hc_networks VALUES (600, 'QHN', 'Queens Health');
INSERT INTO dim_hc_networks VALUES (700, 'SBN', 'South Brooklyn');
INSERT INTO dim_hc_networks VALUES (800, 'SMN', 'South Manhattan');
--INSERT INTO dim_hc_networks VALUES('NYH', 'New York Health Organization');
--INSERT INTO dim_hc_networks VALUES('USH', 'United States Health Organization');




COMMIT;