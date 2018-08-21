CREATE OR REPLACE VIEW V_FACT_RESULTS_HIST
AS
SELECT --+ ordered use_hash(v pe prc e r rf pef vd) cardinality(pat 5000000)
-- 15-Jun-2018, GK: Added data_element_name to the script
-- 15-Mar-2018, OK: created
r.network,
r.visit_id,
r.event_id,
r.result_report_number,
r.multi_field_occurrence_number,
r.item_number,
e.date_time AS result_dt,
v.visit_key,
v.patient_key,
v.patient_id,
NVL(pef.facility_key, prc.facility_key) AS proc_facility_key,
prc.proc_key,
pe.modified_proc_name,
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
dim_patients pat
JOIN fact_visits v ON v.network = pat.network AND v.patient_id = pat.patient_id
JOIN proc_event_history pe ON pe.network = v.network AND pe.visit_id = v.visit_id
JOIN dim_procedures prc
 ON prc.network = pe.network AND prc.src_proc_id = pe.proc_id AND prc.source = 'QCPR'
JOIN event_history e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id
JOIN result_history_cdw r ON r.network = e.network AND r.visit_id = e.visit_id AND r.event_id = e.event_id
JOIN result_field rf ON rf.network = r.network AND rf.data_element_id = r.data_element_id
LEFT JOIN dim_hc_facilities pef ON pef.network = pe.network AND pef.facility_id = pe.facility_id
LEFT JOIN value_decode vd
 ON vd.network = r.network AND vd.decode_source_id = rf.decode_source_id AND vd.encoded_value = r.VALUE
WHERE
pat.current_flag = 1
AND r.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK');