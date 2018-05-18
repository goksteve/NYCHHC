INSERT INTO
 meta_changes a(change_id, comments)
 SELECT
  MAX(change_id) + 1, 'SG HAS CHNGES'
 FROM
  meta_changes;


--INSERT INTO
-- meta_criteria(criterion_id, criterion_cd, description)
-- SELECT
--  MAX(criterion_id) + 1,
--  'DIAGNOSES:MYOCARDIAL INFARCTION (MI)',
--  'List The list of Myocardial Infarction (MI) diagnoses'
-- FROM
--  meta_criteria;
--
--INSERT INTO
-- meta_criteria(criterion_id, criterion_cd, description)
-- SELECT
--  MAX(criterion_id) + 1,
--  'DIAGNOSES:ISCHEMIC VASCULAR DISEASE (IVD)',
--  'List The list of Ischemic Vascular (IVD) diagnoses'
-- FROM
--  meta_criteria;

INSERT INTO
 meta_criteria(criterion_id, criterion_cd, description)
 SELECT
  MAX(criterion_id) + 1,
  'MEDICATIONS:STATIN MEDICATIONS',
  'The list of Statin Medications'
 FROM
  meta_criteria;


INSERT INTO
 meta_criteria(criterion_id, criterion_cd, description)
 SELECT
  MAX(criterion_id) + 1,
  'DIAGNOSES:PREGNANCY ICD CODES',
  'The List Pregnancy Icd Codes Diagnoses'
 FROM
  meta_criteria;


UPDATE meta_conditions
set include_exclude_ind  = 'I'
where criterion_id  = 73;