CREATE TABLE tr13_ref_drug_descriptions
(
  drug_description  VARCHAR2(512 BYTE),
  drug_type_id      NUMBER(6)                   NOT NULL,
  route VARCHAR2(512 BYTE)
);


INSERT INTO tr13_ref_drug_descriptions
WITH dscr as 
(
  SELECT --+ materialize
    DISTINCT drug_description 
  FROM fact_prescriptions
  WHERE drug_description NOT LIKE 'catalyst 5 wheechair dimension%'
)
SELECT
 distinct d.drug_description, c.criterion_id,e.route
FROM dscr d
JOIN meta_conditions c
  ON c.condition_type_cd = 'MED' 
 AND include_exclude_ind = 'I' 
 AND c.comparison_operator = 'LIKE' 
 AND d.drug_description LIKE c.value
 AND c.criterion_id IN (41,44,45,46)
JOIN meta_med_route e
  ON e.criterion_id=c.criterion_id
 AND e.value=c.value ;