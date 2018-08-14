DROP TABLE dsrip_tr047_stat_pharm_cdw;

CREATE TABLE DSRIP_TR047_STAT_PHARM_CDW
(
 report_dt         DATE NOT NULL,
 network           CHAR(3 BYTE) NOT NULL,
 facility_name     VARCHAR2(100 BYTE) NOT NULL,
 facility_cd       VARCHAR2(100 BYTE) NOT NULL,
 patient_id        NUMBER(12) NOT NULL,
 name              VARCHAR2(150 BYTE) NOT NULL,
 birthdate         DATE,
 age               NUMBER(3),
 sex               VARCHAR2(8 BYTE),
 mrn               VARCHAR2(512 BYTE),
 apt_suite         VARCHAR2(1024 BYTE),
 street_address    VARCHAR2(1024 BYTE),
 city              VARCHAR2(50 BYTE),
 state             VARCHAR2(50 BYTE),
 country           VARCHAR2(50 BYTE),
 mailing_code      VARCHAR2(50 BYTE),
 home_phone        VARCHAR2(50 BYTE),
 day_phone         VARCHAR2(50 BYTE),
 cell_phone        VARCHAR2(50 BYTE),
 insurance_name    VARCHAR2(100 BYTE),
 insurance_plan    VARCHAR2(100 BYTE),
 membernum         VARCHAR2(100 BYTE),
 dateordered       DATE,
 service_dt        DATE,
 ndcnum            NUMBER(20),
 drugname          VARCHAR2(1024 BYTE),
 quantity          NUMBER(5),
 dayssupply        NUMBER(5),
 refillnum         number(5),
 prescriberid      NUMBER(20),
 preslastname      VARCHAR2(1024 BYTE),
 presfirstname     VARCHAR2(1024 BYTE),
 pharmacynum       NUMBER(18),
 pharmacyname      VARCHAR2(1024 BYTE),
 pharmacyaddress   VARCHAR2(1024 BYTE),
 pharmacycity      VARCHAR2(1024 BYTE),
 pharmacystate     VARCHAR2(100 BYTE),
 pharmacyzip       VARCHAR2(100 BYTE),
 DSRIP_REPORT VARCHAR2 (255 Byte) DEFAULT 'DSRIP_TR047',
 load_dt           DATE DEFAULT SYSDATE NOT NULL

);

GRANT SELECT ON dsrip_tr047_stat_pharm_cdw TO PUBLIC WITH GRANT OPTION;