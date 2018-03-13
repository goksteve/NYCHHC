CREATE OR REPLACE VIEW v_dim_discharge_types AS
 SELECT
  p.network,
  p.visit_type_id,
  p.discharge_type_id,
  p.name,
  p.facility_id
 FROM
  discharge_type p;
/