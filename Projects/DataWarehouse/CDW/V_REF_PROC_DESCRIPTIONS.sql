CREATE OR REPLACE VIEW v_ref_proc_descriptions AS
 WITH -- 06-Jun-2018, GK: created
     dscr AS
       (
        SELECT --+ materialize
         DISTINCT proc_key,network, src_proc_id, proc_name
        FROM
         dim_procedures
        ),
      cnd AS
       (SELECT --+ materialize
         DISTINCT LOWER(cnd.VALUE) AS VALUE, cr.criterion_id proc_type_id, include_exclude_ind AS in_ind
        FROM
         meta_criteria cr JOIN meta_conditions cnd ON cnd.criterion_id = cr.criterion_id
        WHERE
         cr.criterion_cd LIKE 'PROCEDURES%')
 SELECT /* parallel(32 */
    proc_key,
    network,
    src_proc_id as proc_id,
    proc_name,
    proc_type_id,
    in_ind
 FROM
  (
   SELECT --+ ordered parallel(32)
    proc_key,
    network,
    src_proc_id, 
    d.proc_name,
    c.proc_type_id,
    in_ind,
    ROW_NUMBER() OVER(PARTITION BY proc_key, d.proc_name, c.proc_type_id ORDER BY in_ind) cnt
   FROM
    dscr d JOIN cnd c ON d.proc_name LIKE c.VALUE
  )
 WHERE
  cnt = 1;
/