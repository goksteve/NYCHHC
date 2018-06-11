exec dbm.drop_tables('CNF_DW_REFRESH');
 
CREATE TABLE cnf_dw_refresh
(
  etl_step_num      NUMBER(10) CONSTRAINT pk_cnf_dw_refresh PRIMARY KEY,
  operation         VARCHAR2(100 BYTE) NOT NULL,       
  target_table      VARCHAR2(30 BYTE) NOT NULL,
  data_source       VARCHAR2(512 BYTE) NOT NULL,
  uk_col_list       VARCHAR2(256 BYTE),
  where_clause      VARCHAR2(2000 BYTE),
  delete_condition  VARCHAR2(2000 BYTE),
  error_table       VARCHAR2(30 BYTE)
);

GRANT SELECT ON cnf_dw_refresh TO PUBLIC;

truncate table cnf_dw_refresh;

INSERT INTO cnf_dw_refresh VALUES(401, 'MERGE /*+ INDEX(t) */', 'PROC', 'SELECT * FROM proc_cbnd', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(402, 'MERGE /*+ INDEX(t) */', 'PROC', 'SELECT * FROM proc_gp1d', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(403, 'MERGE /*+ INDEX(t) */', 'PROC', 'SELECT * FROM proc_gp2d', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(404, 'MERGE /*+ INDEX(t) */', 'PROC', 'SELECT * FROM proc_nbnd', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(405, 'MERGE /*+ INDEX(t) */', 'PROC', 'SELECT * FROM proc_nbxd', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(406, 'MERGE /*+ INDEX(t) */', 'PROC', 'SELECT * FROM proc_qhnd', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(407, 'MERGE /*+ INDEX(t) */', 'PROC', 'SELECT * FROM proc_sbnd', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(408, 'MERGE /*+ INDEX(t) */', 'PROC', 'SELECT * FROM proc_smnd', NULL, NULL, NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(411, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT', 'SELECT * FROM visit_cbnd', NULL, NULL, NULL, 'ERR_VISIT');
INSERT INTO cnf_dw_refresh VALUES(412, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT', 'SELECT * FROM visit_gp1d', NULL, NULL, NULL, 'ERR_VISIT');
INSERT INTO cnf_dw_refresh VALUES(413, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT', 'SELECT * FROM visit_gp2d', NULL, NULL, NULL, 'ERR_VISIT');
INSERT INTO cnf_dw_refresh VALUES(414, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT', 'SELECT * FROM visit_nbnd', NULL, NULL, NULL, 'ERR_VISIT');
INSERT INTO cnf_dw_refresh VALUES(415, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT', 'SELECT * FROM visit_nbxd', NULL, NULL, NULL, 'ERR_VISIT');
INSERT INTO cnf_dw_refresh VALUES(416, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT', 'SELECT * FROM visit_qhnd', NULL, NULL, NULL, 'ERR_VISIT');
INSERT INTO cnf_dw_refresh VALUES(417, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT', 'SELECT * FROM visit_sbnd', NULL, NULL, NULL, 'ERR_VISIT');
INSERT INTO cnf_dw_refresh VALUES(418, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT', 'SELECT * FROM visit_smnd', NULL, NULL, NULL, 'ERR_VISIT');

INSERT INTO cnf_dw_refresh VALUES(421, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT', 'SELECT vs.* FROM visit_segment_cbnd vs JOIN visit v ON v.network = vs.network AND v.visit_id = vs.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(422, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT', 'SELECT vs.* FROM visit_segment_gp1d vs JOIN visit v ON v.network = vs.network AND v.visit_id = vs.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(423, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT', 'SELECT vs.* FROM visit_segment_gp2d vs JOIN visit v ON v.network = vs.network AND v.visit_id = vs.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(424, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT', 'SELECT vs.* FROM visit_segment_nbnd vs JOIN visit v ON v.network = vs.network AND v.visit_id = vs.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(425, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT', 'SELECT vs.* FROM visit_segment_nbxd vs JOIN visit v ON v.network = vs.network AND v.visit_id = vs.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(426, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT', 'SELECT vs.* FROM visit_segment_qhnd vs JOIN visit v ON v.network = vs.network AND v.visit_id = vs.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(427, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT', 'SELECT vs.* FROM visit_segment_sbnd vs JOIN visit v ON v.network = vs.network AND v.visit_id = vs.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(428, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT', 'SELECT vs.* FROM visit_segment_smnd vs JOIN visit v ON v.network = vs.network AND v.visit_id = vs.visit_id', NULL, NULL, NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(431, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT_VISIT_LOCATION', 'SELECT vsl.* FROM visit_segment_visit_locat_cbnd vsl JOIN visit v ON v.network = vsl.network AND v.visit_id = vsl.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(432, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT_VISIT_LOCATION', 'SELECT vsl.* FROM visit_segment_visit_locat_gp1d vsl JOIN visit v ON v.network = vsl.network AND v.visit_id = vsl.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(433, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT_VISIT_LOCATION', 'SELECT vsl.* FROM visit_segment_visit_locat_gp2d vsl JOIN visit v ON v.network = vsl.network AND v.visit_id = vsl.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(434, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT_VISIT_LOCATION', 'SELECT vsl.* FROM visit_segment_visit_locat_nbnd vsl JOIN visit v ON v.network = vsl.network AND v.visit_id = vsl.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(435, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT_VISIT_LOCATION', 'SELECT vsl.* FROM visit_segment_visit_locat_nbxd vsl JOIN visit v ON v.network = vsl.network AND v.visit_id = vsl.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(436, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT_VISIT_LOCATION', 'SELECT vsl.* FROM visit_segment_visit_locat_qhnd vsl JOIN visit v ON v.network = vsl.network AND v.visit_id = vsl.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(437, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT_VISIT_LOCATION', 'SELECT vsl.* FROM visit_segment_visit_locat_sbnd vsl JOIN visit v ON v.network = vsl.network AND v.visit_id = vsl.visit_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(438, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'VISIT_SEGMENT_VISIT_LOCATION', 'SELECT vsl.* FROM visit_segment_visit_locat_smnd vsl JOIN visit v ON v.network = vsl.network AND v.visit_id = vsl.visit_id', NULL, NULL, NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(501, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'EVENT', 'SELECT * FROM event_cbnd', NULL, NULL, NULL, 'ERR_EVENT');
INSERT INTO cnf_dw_refresh VALUES(502, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'EVENT', 'SELECT * FROM event_gp1d', NULL, NULL, NULL, 'ERR_EVENT');
INSERT INTO cnf_dw_refresh VALUES(503, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'EVENT', 'SELECT * FROM event_gp2d', NULL, NULL, NULL, 'ERR_EVENT');
INSERT INTO cnf_dw_refresh VALUES(504, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'EVENT', 'SELECT * FROM event_nbnd', NULL, NULL, NULL, 'ERR_EVENT');
INSERT INTO cnf_dw_refresh VALUES(505, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'EVENT', 'SELECT * FROM event_nbxd', NULL, NULL, NULL, 'ERR_EVENT');
INSERT INTO cnf_dw_refresh VALUES(506, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'EVENT', 'SELECT * FROM event_qhnd', NULL, NULL, NULL, 'ERR_EVENT');
INSERT INTO cnf_dw_refresh VALUES(507, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'EVENT', 'SELECT * FROM event_sbnd', NULL, NULL, NULL, 'ERR_EVENT');
INSERT INTO cnf_dw_refresh VALUES(508, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'EVENT', 'SELECT * FROM event_smnd', NULL, NULL, NULL, 'ERR_EVENT');

INSERT INTO cnf_dw_refresh VALUES(511, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT', 'SELECT pe.* FROM proc_event_cbnd pe JOIN event e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(512, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT', 'SELECT pe.* FROM proc_event_gp1d pe JOIN event e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(513, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT', 'SELECT pe.* FROM proc_event_gp2d pe JOIN event e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(514, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT', 'SELECT pe.* FROM proc_event_nbnd pe JOIN event e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(515, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT', 'SELECT pe.* FROM proc_event_nbxd pe JOIN event e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(516, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT', 'SELECT pe.* FROM proc_event_qhnd pe JOIN event e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(517, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT', 'SELECT pe.* FROM proc_event_sbnd pe JOIN event e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(518, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT', 'SELECT pe.* FROM proc_event_smnd pe JOIN event e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id', NULL, NULL, NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(521, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'RESULT', 'SELECT r.* FROM result_cbnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(522, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'RESULT', 'SELECT r.* FROM result_gp1d r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(523, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'RESULT', 'SELECT r.* FROM result_gp2d r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(524, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'RESULT', 'SELECT r.* FROM result_nbnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(525, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'RESULT', 'SELECT r.* FROM result_nbxd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(526, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'RESULT', 'SELECT r.* FROM result_qhnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(527, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'RESULT', 'SELECT r.* FROM result_sbnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(528, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'RESULT', 'SELECT r.* FROM result_smnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(531, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT_ARCHIVE', 'SELECT r.* FROM proc_event_archive_cbnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(532, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT_ARCHIVE', 'SELECT r.* FROM proc_event_archive_gp1d r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(533, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT_ARCHIVE', 'SELECT r.* FROM proc_event_archive_gp2d r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(534, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT_ARCHIVE', 'SELECT r.* FROM proc_event_archive_nbnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(535, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT_ARCHIVE', 'SELECT r.* FROM proc_event_archive_nbxd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(536, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT_ARCHIVE', 'SELECT r.* FROM proc_event_archive_qhnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(537, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT_ARCHIVE', 'SELECT r.* FROM proc_event_archive_sbnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(538, 'MERGE /*+ PARALLEL(32) INDEX(t) */', 'PROC_EVENT_ARCHIVE', 'SELECT r.* FROM proc_event_archive_smnd r JOIN event e ON e.network = r.network AND e.visit_id = r.visit_id AND e.event_id = r.event_id', NULL, NULL, NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(710, 'MERGE', 'REF_ORDER_TYPES', 'SELECT DISTINCT order_type_id, name FROM order_type', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(720, 'MERGE', 'REF_VISIT_TYPES', 'SELECT DISTINCT visit_type_id, name, abbreviation FROM visit_type', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(730, 'MERGE', 'REF_VISIT_SERVICE_TYPES', 'SELECT network,visit_type_id,visit_service_type_id,name,short_name,facility_id FROM visit_service_type WHERE active = ''active''', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(740, 'MERGE', 'REF_HC_SPECIALTIES', 'V_REF_HC_SPECIALTIES', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(750, 'MERGE', 'REF_PROC_EVENT_ARCHIVE_TYPES', 'SELECT DISTINCT archive_type_id, name FROM proc_event_archive_type', NULL, 'WHERE archive_type_id <> 0', NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(760, 'MERGE', 'REF_FINANCIAL_CLASS', 'SELECT network,financial_class_id,name FROM financial_class', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(770, 'MERGE /*+ PARALLEL(32)*/', 'REF_DIAGNOSES', 'V_REF_DIAGNOSES', NULL, NULL, 'DELETE_FLAG=''Y''', NULL);


INSERT INTO cnf_dw_refresh VALUES(820, 'MERGE', 'DIM_DISCHARGE_TYPES', 'V_DIM_DISCHARGE_TYPES', 'NETWORK,VISIT_TYPE_ID,DISCHARGE_TYPE_ID', NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(850, 'MERGE', 'DIM_PAYERS', 'PT008.PAYER_MAPPING', 'NETWORK,PAYER_ID', NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(895, 'EQUALIZE /*+ PARALLEL(32)*/', 'MAP_DIAGNOSES_CODES', 'V_MAP_DIAGNOSES_CODES', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(900, 'MERGE /*+ PARALLEL(32)*/', 'DIM_HC_DEPARTMENTS', 'V_DIM_HC_DEPARTMENTS', 'NETWORK,LOCATION_ID', NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(905, 'MERGE /*+ PARALLEL(32)*/', 'DIM_PROCEDURES', 'V_DIM_PROCEDURES', 'NETWORK,SRC_PROC_ID', NULL, NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(1010, 'INSERT /*+ PARALLEL(32)*/', 'V_DIM_PATIENTS', 'V_DIM_PATIENTS', NULL, 'WHERE change IS NOT NULL', NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(1020, 'INSERT /*+ PARALLEL(32)*/', 'V_DIM_PROVIDERS', 'V_DIM_PROVIDERS', NULL, 'WHERE change IS NOT NULL', NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(1101, 'REPLACE /*+ PARALLEL(32)*/', 'FACT_PATIENT_PRESCRIPTIONS', 'V_FACT_PATIENT_PRESCRIPTIONS', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(1102, 'REPLACE /*+ PARALLEL(32)*/', 'REF_DRUG_DESCRIPTIONS', 'V_REF_DRUG_DESCRIPTIONS', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(1103, 'REPLACE /*+ PARALLEL(32)*/', 'REF_DRUG_NAMES', 'V_REF_DRUG_NAMES', NULL, NULL, NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(2000, 'REPLACE /*+ PARALLEL(32)*/', 'FACT_VISITS', 'V_FACT_VISITS_FULL', NULL, NULL, NULL, 'ERR_FACT_VISITS');
--INSERT INTO cnf_dw_refresh VALUES(2010, 'MERGE /*+ PARALLEL(32)*/', 'FACT_VISITS', 'V_FACT_VISITS_INCREMENTAL', NULL, NULL, NULL, 'ERR_FACT_VISITS');
INSERT INTO cnf_dw_refresh VALUES(2020, 'REPLACE /*+ PARALLEL(32)*/', 'FACT_VISIT_DIAGNOSES', 'V_FACT_VISIT_DIAGNOSES', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(2030, 'REPLACE /*+ PARALLEL(32)*/', 'FACT_VISIT_SEGMENT_LOCATIONS', 'V_FACT_VISIT_SEGMENT_LOCATIONS', NULL, NULL, NULL, NULL);
--INSERT INTO cnf_dw_refresh VALUES(2040, 'REPLACE /*+ PARALLEL(32)*/', 'STG_PATIENT_SECONDARY_NUMBER', 'V_STG_PATIENT_SECONDARY_NUMBER', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(2040, 'REPLACE /*+ PARALLEL(32)*/', 'REF_PATIENT_SECONDARY_MRN', 'V_REF_PATIENT_SECONDARY_MRN', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(2110, 'REPLACE /*+ PARALLEL(32)*/', 'FACT_PATIENT_DIAGNOSES', 'V_FACT_PATIENT_DIAGNOSES', NULL, NULL, NULL, NULL);

INSERT INTO cnf_dw_refresh VALUES(3110, 'REPLACE /*+ PARALLEL(32) APPEND */', 'FACT_RESULTS', 'V_FACT_RESULTS_FULL', NULL, 'WHERE network=''GP1''', NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(3120, 'INSERT /*+ PARALLEL(32) APPEND */', 'FACT_RESULTS', 'V_FACT_RESULTS_FULL', NULL, 'WHERE network=''QHN''', NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(3130, 'INSERT /*+ PARALLEL(32) APPEND */', 'FACT_RESULTS', 'V_FACT_RESULTS_FULL', NULL, 'WHERE network=''SMN''', NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(3140, 'INSERT /*+ PARALLEL(32) APPEND */', 'FACT_RESULTS', 'V_FACT_RESULTS_FULL', NULL, 'WHERE network=''NBX''', NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(3150, 'INSERT /*+ PARALLEL(32) APPEND */', 'FACT_RESULTS', 'V_FACT_RESULTS_FULL', NULL, 'WHERE network IN (''GP2'',''SBN'')', NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES(3160, 'INSERT /*+ PARALLEL(32) APPEND */', 'FACT_RESULTS', 'V_FACT_RESULTS_FULL', NULL, 'WHERE network IN (''CBN'',''NBN'')', NULL, NULL);

--INSERT INTO cnf_dw_refresh VALUES(3210, 'MERGE /*+ PARALLEL(32)*/', 'FACT_RESULTS', 'V_FACT_RESULTS', NULL, 'WHERE network=''GP1''', NULL, NULL);
--INSERT INTO cnf_dw_refresh VALUES(3220, 'MERGE /*+ PARALLEL(32)*/', 'FACT_RESULTS', 'V_FACT_RESULTS', NULL, 'WHERE network=''QHN''', NULL, NULL);
--INSERT INTO cnf_dw_refresh VALUES(3230, 'MERGE /*+ PARALLEL(32)*/', 'FACT_RESULTS', 'V_FACT_RESULTS', NULL, 'WHERE network=''SMN''', NULL, NULL);
--INSERT INTO cnf_dw_refresh VALUES(3240, 'MERGE /*+ PARALLEL(32)*/', 'FACT_RESULTS', 'V_FACT_RESULTS', NULL, 'WHERE network=''NBX''', NULL, NULL);
--INSERT INTO cnf_dw_refresh VALUES(3250, 'MERGE /*+ PARALLEL(32)*/', 'FACT_RESULTS', 'V_FACT_RESULTS', NULL, 'WHERE network IN (''GP2'',''SBN'')', NULL, NULL);
--INSERT INTO cnf_dw_refresh VALUES(3260, 'MERGE /*+ PARALLEL(32)*/', 'FACT_RESULTS', 'V_FACT_RESULTS', NULL, 'WHERE network IN (''CBN'',''NBN'')', NULL, NULL);

--4000 series for metric  daily data refresh
INSERT INTO cnf_dw_refresh VALUES (4000, 'REPLACE /*+ APPEND PARALLEL(32) */', 'STG_VISIT_METRICS_EPIC', 'V_STG_VISIT_METRICS_EPIC', NULL, NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES (4100, 'REPLACE /*+ APPEND PARALLEL(32) */', 'FACT_VISIT_METRICS', 'V_FACT_VISIT_METRICS', NULL,  NULL, NULL, NULL);
INSERT INTO cnf_dw_refresh VALUES (4110, 'REPLACE /*+ APPEND PARALLEL(32) */', 'FACT_VISIT_MONTHLY_METRICS', 'V_FACT_VISIT_MONTHLY_METRICS', NULL,  NULL, NULL, NULL);
--5000 series for QCPR daily data refresh
--INSERT INTO cnf_dw_refresh VALUES(5000, 'REPLACE /*+ INDEX(t) */', 'fact_daily_visits_stats_qmed', 'v_fact_daily_visits_stats_qmed', NULL, NULL, NULL, NULL);
COMMIT;