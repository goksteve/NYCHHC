CREATE OR REPLACE VIEW v_stg_proc_results AS
 WITH tmp_proc AS
       (SELECT --+ materialize
         pe.network,
         pe.visit_id,
         pe.event_id,
         pe.proc_id,
         pe.facility_id,
         pe.modified_proc_name,
         pe.cid
        FROM
         proc_event pe
         LEFT JOIN fact_results r
          ON r.network = pe.network AND r.visit_id = pe.visit_id AND r.event_id = pe.event_id
        WHERE
         1 = 1
         AND pe.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
         AND r.network IS NULL
         AND r.visit_id IS NULL
         AND r.event_id IS NULL)
 SELECT --+  PARALLEL (48)
  pe.network,
  pe.visit_id,
  pe.event_id,
  1 AS result_report_number,
  1 AS multi_field_occurrence_number,
  1 AS item_number,
  e.date_time AS result_dt,
  v.visit_key,
  v.patient_key,
  v.patient_id,
  NVL(pef.facility_key, prc.facility_key) AS proc_facility_key,
  prc.proc_key,
  pe.modified_proc_name,
  e.event_status_id,
  e.event_type_id,
  -99 AS data_element_id,
  'n/a' AS data_element_name,
  'n/a' AS result_value,
  NULL AS decode_source_id,
  NULL decoded_value,
  pe.cid --,
 -- SYSDATE AS load_dt
 FROM
  tmp_proc pe
  JOIN fact_visits v ON pe.network = v.network AND pe.visit_id = v.visit_id
  JOIN dim_procedures prc
   ON prc.network = pe.network AND prc.src_proc_id = pe.proc_id AND prc.source = 'QCPR'
  JOIN event e ON e.network = pe.network AND e.visit_id = pe.visit_id AND e.event_id = pe.event_id
  LEFT JOIN dim_hc_facilities pef ON pef.network = pe.network AND pef.facility_id = pe.facility_id
 WHERE
  1 = 1 AND e.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')