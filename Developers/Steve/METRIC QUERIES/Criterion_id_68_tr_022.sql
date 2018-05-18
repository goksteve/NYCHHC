
INSERT INTO
 meta_changes(change_id, comments)
VALUES
 (68, 'Added criterion_id  68');


INSERT INTO
 meta_conditions
 SELECT /*+ parallel (32) */
  68 AS criterion_id,
  network,
  'NONE' qualifier,
  data_element_id AS VALUE,
  name AS value_description,
  'EI' AS condition_type_cd,
  '=' comparison_operator,
  'I' include_exclude_ind
 FROM
  result_field
 WHERE
  name LIKE '%Retina%'
  OR name LIKE '%Comprehensive%dilated%eye%exam%'
  OR name LIKE '%Tonometry%'
  OR name LIKE '%Visual field%'
  OR name LIKE '%Fluorescein%angiogram%'
  OR name LIKE '%Pupillary%dilation%'
  OR name LIKE '%Ophthalmoscopy%'
  OR name LIKE '%Gonioscopy%'
  OR name LIKE '%Nerve%fiber%analysis%'
  OR name LIKE '%Pachymetry%';