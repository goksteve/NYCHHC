CREATE OR REPLACE VIEW v_ref_proc_descriptions 
AS
WITH
 -- 06-Jun-2018, GK: created
  dscr as
  (
    SELECT --+ materialize
      DISTINCT proc_name
    FROM dim_procedures
  ),
  cnd AS
  (
    SELECT --+ materialize
      DISTINCT
   LOWER(cnd.value) as value,
      cr.criterion_id proc_type_id
    FROM meta_criteria cr
    JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
    WHERE cr.criterion_cd LIKE 'PROCEDURES%' AND cr.criterion_id >= 87
  )
SELECT --+ ordered parallel(32)
  DISTINCT d.proc_name, c.proc_type_id
FROM dscr d
JOIN cnd c ON d.proc_name LIKE c.value;