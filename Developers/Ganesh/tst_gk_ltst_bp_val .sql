CREATE OR REPLACE TYPE obj_bp_rslt AS OBJECT (p_systolic varchar2(256), p_diastolic varchar2(256), p_result_dt date);
CREATE OR REPLACE TYPE tab_bp_rslt IS TABLE OF obj_bp_rslt;

CREATE OR REPLACE FUNCTION tst_gk_ltst_bp_val(p_network_in fact_visits.network%TYPE, p_visit_id_in fact_visits.visit_id%TYPE)
RETURN tab_bp_rslt
IS
list_bp_rslt tab_bp_rslt;
BEGIN

  WITH 
    htn_rslt_lkp AS
    (
      SELECT 
        mc.network,
        mc.VALUE,
        CASE WHEN UPPER (mc.value_description) LIKE '%SYS%' THEN 'S' -- systolic
           WHEN UPPER (mc.value_description) LIKE '%DIAS%' THEN 'D' -- diastolic
           ELSE 'C' -- combo
        END test_type
      FROM meta_conditions mc --AND mc.criterion_id = 13 AND mc.include_exclude_ind = 'I';	
    ),
    rslt AS
    (
      SELECT
        r.network,
        r.patient_id,
        r.visit_id,
        r.result_value,
        r.event_id,
        r.result_dt,
        CASE
          WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',1))
          WHEN lkp.test_type = 'S' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
        END AS systolic_bp,
        CASE
          WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_VALUE,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',2))
          WHEN lkp.test_type = 'D' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
        END AS diastolic_bp 
      FROM fact_results r
      JOIN htn_rslt_lkp lkp 
        ON r.data_element_id = lkp.value
       AND r.network = lkp.network
       WHERE r.visit_id = p_visit_id_in AND r.network = p_network_in
    ),
     rslt_combo AS
    (
      SELECT visit_id,result_dt,systolic_bp,diastolic_bp,
      row_number() over (partition by network,visit_id order by result_dt desc) rnum_per_visit 
        FROM 
        (
          SELECT
            r.network,
            r.visit_id,
            r.event_id,
            r.result_dt,
            MAX (systolic_bp) systolic_bp,
            MAX (diastolic_bp) diastolic_bp
           -- ROW_NUMBER() OVER (PARTITION BY visit_id, TRUNC (result_dt) ORDER BY result_dt DESC) rnum_per_day
          FROM rslt r
          GROUP BY r.network,r.visit_id,r.event_id,r.result_dt
          HAVING MAX (systolic_bp) BETWEEN 0 AND 311 AND MAX (diastolic_bp) BETWEEN 0 AND 284
         ) 
    )
  SELECT obj_bp_rslt(systolic_bp,diastolic_bp,result_dt) bulk collect into list_bp_rslt FROM rslt_combo where rnum_per_visit=1 ;
  RETURN list_bp_rslt;

END;