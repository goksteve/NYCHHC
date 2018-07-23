drop table  stg_new_prescr; 
CREATE TABLE stg_new_prescr AS
 SELECT /*+ parallel( 32) */
 DISTINCT
  pm.network,
  pp.patient_key,
  999999999 AS facility_id,
  'N/A' AS facility_name,
  TO_NUMBER(TO_CHAR(NVL(pm.order_date_time, DATE '2010-01-01'), 'YYYYMMDD')) AS datenum_order_dt_key,
  999999999 AS provider_id,
  CAST( pm.patient_id AS VARCHAR2(256)) patient_id,
  pma.assoc_visit_id AS order_visit_id,
  -99 AS order_event_id,
  -99 AS order_span_id,
  TRUNC(pm.order_date_time) AS order_dt,
  'N/A' AS drug_description,
  pm.dosage,
  pm.frequency,
  pm.quantity,
  pm.refills,
  'N/A' as misc_name,
  st.name AS prescription_status,
  'N/A'  AS prescription_type,
  pm.medication_id AS proc_id,
  pm.dkv_drug_name AS drug_name,
  pm.allow_substitution AS rx_allow_subs,
  pm.comments || ', ' || pm.continue_comment as  RX_COMMENT  ,
  pm.stop_date_approx AS rx_dc_time,
  pm.rx_disposition AS rx_dispo,
  DATE '2099-01-01' rx_exp_date,
  -99 AS rx_id,
  '-99' AS rx_number,
  trunc(pm.start_date) start_date,
   trunc(pm.stop_date) stop_date,
   pm.dkv_dnid_name || ', ' || pm.dkv_generic_dnid_name || ', ' || pm.dkv_product_string AS derived_product_name
 FROM
      dim_patients pp
  JOIN patient_med pm ON pp.patient_id = pm.patient_id AND pp.network = pm.network AND pp.current_flag = 1
  JOIN patient_med_archive_med pmam ON pm.patient_id = pmam.patient_id AND pm.network = pmam.network and pmam.medication_id = pm.medication_id
  JOIN patient_med_archive pma  ON pmam.patient_id = pma.patient_id  AND pmam.network = pma.network      AND pmam.medication_archive_id = pma.medication_archive_id
  --LEFT JOIN patient_med_last_action_taken lat   ON pmam.last_action_taken_id = lat.last_action_taken_id AND pmam.network = lat.network
  LEFT JOIN e_rx_status st ON pm.e_rx_status_id = st.e_rx_status_id AND pm.network = st.network
 WHERE
  1 = 1
  AND  (pma.archive_date_time >= '01-NOV-2017' OR pmam.order_date_time >= '01-NOV-2017')
  AND pmam.last_action_taken_id <> 4;