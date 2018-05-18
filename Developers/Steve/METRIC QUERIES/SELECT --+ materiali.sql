SELECT --+ materialize
 DISTINCT cnd.VALUE AS VALUE,
CASE
  WHEN cr.criterion_id IN (1,6,50,51,52,58,60) THEN            'diabetes'
  WHEN cr.criterion_id IN (21,48,49,53,57,59) THEN            'asthma'
  WHEN cr.criterion_id IN (7,9,31,32) THEN            'bh'
  WHEN cr.criterion_id IN (17,8) THEN            'breast_cancer'
  WHEN cr.criterion_id IN (27) THEN            'cervical_cancer'
  WHEN cr.criterion_id IN (39) THEN            'heart_failure'
  WHEN cr.criterion_id IN (3,38) THEN             'hypertansion'
  WHEN cr.criterion_id IN (63, 65) THEN            'kidney_ diseases'
END   AS diag_type_ind,
    cr.criterion_id diag_type_id,
    cr.criterion_cd
FROM
 meta_criteria cr JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
WHERE
 cr.criterion_id IN (1,3,6,7,9,11,17,18,21, 7,31,32,38,39,48,49,50,51,52,53,57,58,59,60,63,65) and INCLUDE_EXCLUDE_IND  = 'I'