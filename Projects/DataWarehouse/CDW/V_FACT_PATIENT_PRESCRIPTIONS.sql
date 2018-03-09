CREATE OR REPLACE VIEW v_fact_patient_prescriptions 
-- 08-Mar-2018, SG: created
AS
 SELECT
  network,
  facility_id,
  patient_id,
  medical_record_number AS mrn,
  NVL(order_time, DATE '2010-01-01') AS order_dt,
  LOWER(procedure_name) AS drug_name,
  LOWER(derived_product_name) AS drug_description,
  rx_quantity,
  dosage,
  frequency,
  rx_dc_time AS rx_dc_dt,
  rx_exp_date AS rx_exp_dt
 FROM
  pt008.prescription_detail;