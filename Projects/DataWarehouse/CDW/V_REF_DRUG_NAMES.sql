CREATE OR REPLACE VIEW v_ref_drug_names AS
WITH
 -- 10-Mar-2018, OK: created
  nm as
  (
    SELECT --+ materialize
      DISTINCT drug_name
    FROM fact_patient_prescriptions
  ),
  cnd AS
  (
    SELECT --+ materialize
      DISTINCT
      cnd.value,
      cr.criterion_id drug_type_id
    FROM meta_criteria cr
    JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
    WHERE cr.criterion_cd LIKE 'MEDICATIONS%'
  )
SELECT --+ ordered
  DISTINCT n.drug_name, c.drug_type_id
FROM nm n
JOIN cnd c ON n.drug_name LIKE c.value;
