CREATE OR REPLACE VIEW v_stg_prescription_details AS
 SELECT
  d.network,
  d.patient_id,
  TO_NUMBER(TO_CHAR(NVL(d.order_time, DATE '2010-01-01'), 'YYYYMMDD')) AS order_dt_key,
  facility_id,
  NVL(d.order_time, DATE '2010-01-01') AS order_dt,
  NVL(LOWER(d.procedure_name), 'n/a') AS drug_name,
  NVL(LOWER(d.derived_product_name), 'n/a') AS drug_description,
  d.rx_quantity,
  d.dosage,
  d.frequency,
  d.rx_dc_time AS rx_dc_dt,
  d.rx_exp_date AS rx_exp_dt
 FROM
  pt008.prescription_detail d;