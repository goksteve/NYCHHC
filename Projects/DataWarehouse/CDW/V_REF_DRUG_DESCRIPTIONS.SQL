CREATE OR REPLACE VIEW v_ref_drug_descriptions AS
WITH
 -- 08-Mar-2018, SG: created
  dscr as
  (
    SELECT --+ materialize
      DISTINCT drug_description
    FROM fact_patient_prescriptions
  ),
  cnd AS
  (
    SELECT --+ materialize
      DISTINCT
   LOWER(  cnd.value) as value,
      cr.criterion_id drug_type_id
    FROM meta_criteria cr
    JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
    WHERE cr.criterion_cd LIKE 'MEDICATIONS%'
  )
SELECT --+ ordered
  DISTINCT d.drug_description, c.drug_type_id
FROM dscr d
JOIN cnd c ON d.drug_description LIKE c.value;


 SELECT --+ materialize
      DISTINCT
   LOWER(  cnd.value) as value,
      cr.criterion_id drug_type_id
    FROM meta_criteria cr
    JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
    WHERE cr.criterion_cd LIKE 'MEDICATIONS%'

