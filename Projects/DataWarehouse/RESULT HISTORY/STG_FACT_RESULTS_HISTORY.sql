create table  STG_FACT_RESULTS_HISTORY
nologging
COMPRESS BASIC
PARTITION BY LIST (network)
 SUBPARTITION BY HASH (visit_id)
  SUBPARTITIONS 16
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'))

AS
SELECT --+  parallel (32) ordered use_hash(v pe prc e r rf pef vd) cardinality(pat 5000000)
r.network,
r.visit_id,
r.event_id,
r.result_report_number,
r.multi_field_occurrence_number,
r.item_number,
e.date_time AS result_dt,

--v.visit_key,
--v.patient_key,
--v.patient_id,
--NVL(pef.facility_key, prc.facility_key) AS proc_facility_key,
--prc.proc_key,
pe.modified_proc_name,
pe.facility_id,
pe.proc_id,
e.event_status_id,
e.event_type_id,
r.data_element_id,
rf.name AS data_element_name,
r.VALUE AS result_value,
rf.decode_source_id,
vd.decoded_value,
r.cid,
SYSDATE AS load_dt
FROM
proc_event_history pe
JOIN event_history e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id
JOIN result_history_cdw r ON r.network = e.network AND r.visit_id = e.visit_id AND r.event_id = e.event_id
JOIN result_field rf ON rf.network = r.network AND rf.data_element_id = r.data_element_id
LEFT JOIN dim_hc_facilities pef ON pef.network = pe.network AND pef.facility_id = pe.facility_id
LEFT JOIN value_decode vd ON vd.network = r.network AND vd.decode_source_id = rf.decode_source_id AND vd.encoded_value = r.VALUE
