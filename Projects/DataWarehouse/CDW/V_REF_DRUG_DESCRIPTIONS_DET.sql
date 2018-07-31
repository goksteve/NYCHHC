CREATE OR REPLACE VIEW V_REF_DRUG_DESCRIPTIONS_DET AS
 WITH -- 08-Mar-2018, SG: created
  dscr AS
  (
    SELECT --+ materialize
     DISTINCT orig_drug_description as drug_description
    FROM
     fact_patient_prescriptions_d
  ),
      cnd AS
       (SELECT --+ materialize
         DISTINCT LOWER(cnd.VALUE) AS VALUE, cr.criterion_id drug_type_id
        FROM
         meta_criteria cr JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
        WHERE
         cr.criterion_cd LIKE 'MEDICATIONS%')
 SELECT --+ ordered
  DISTINCT d.drug_description, c.drug_type_id
 FROM
  dscr d JOIN cnd c ON d.drug_description LIKE c.VALUE;