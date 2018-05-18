CREATE TABLE DSRIP_TR002_023_A1C_EPIC
(
 empi                    VARCHAR2(100 CHAR) NOT NULL,
 facility_mrn            VARCHAR2(100 CHAR) NULL,
 pat_last_name           VARCHAR2(200 CHAR) NOT NULL,
 pat_first_name          VARCHAR2(200 CHAR) NOT NULL,
 add_line_1              VARCHAR2(255 CHAR) NULL,
 add_line_2              VARCHAR2(255 CHAR) NULL,
 city                    VARCHAR2(255 CHAR) NULL,
 zip                     VARCHAR2(20 CHAR) NULL,
 state                   VARCHAR2(80 CHAR) NULL,
 pat_home_phone          VARCHAR2(50 CHAR) NULL,
 pat_work_phone          VARCHAR2(50 CHAR) NULL,
 birth_date              DATE NULL,
 age_years               INTEGER NULL,
 inspayor1               VARCHAR2(255 CHAR) NULL,
 inspayor2               VARCHAR2(255 CHAR) NULL,
 inspayor3               VARCHAR2(255 CHAR) NULL,
 facility_latest         VARCHAR2(255 CHAR) NULL,
 encounter_date_latest   DATE NULL,
 result_time_latest      DATE NULL,
 result_value_latest     VARCHAR2(254 CHAR) NULL,
 a1c_less_then_8         VARCHAR2(1 CHAR) NULL,
 a1c_grt_eg_8            VARCHAR2(1 CHAR) NULL,
 a1c_grt_eg_9            VARCHAR2(1 CHAR) NULL,
 a1c_grt_eg_9_or_null    VARCHAR2(1 CHAR) NULL,
 pcp_general             VARCHAR2(200 CHAR) NULL,
 etl_load_date           DATE NOT NULL,
 epic_flag               CHAR(1 CHAR) NULL,
 source                  VARCHAR2(4 CHAR) NULL,
 report_month_dt         DATE DEFAULT TRUNC(SYSDATE, 'MONTH') NULL
)
NOLOGGING
COMPRESS BASIC;

CREATE INDEX idx_tr002_023_a1c_epic1
 ON dsrip_tr002_023_hba1c_8_9_epic(report_month_dt);

ALTER TABLE dsrip_tr002_023_hba1c_8_9_epic
 ADD CONSTRAINT pk_tr002_023_a1c_epic1 PRIMARY KEY(empi, report_month_dt, pat_last_name);