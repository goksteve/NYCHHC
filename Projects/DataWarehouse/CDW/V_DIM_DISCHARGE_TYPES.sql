CREATE OR REPLACE VIEW v_dim_discharge_types AS
 SELECT
  p.network,
  p.visit_type_id,
  p.discharge_type_id,
  p.name as discharge_type_name,
  p.facility_id
 FROM
  discharge_type p;
/