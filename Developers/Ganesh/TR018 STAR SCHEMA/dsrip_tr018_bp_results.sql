WITH 
  dt AS 
  (
    SELECT --+ materialize
      TRUNC(SYSDATE, 'MONTH') AS report_period_start_dt,
      TRUNC(SYSDATE, 'YEAR') AS msrmnt_yr_start_dt,
      ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) begin_dt,
      TRUNC (SYSDATE, 'MONTH') end_dt
    FROM DUAL
  ),
  htn_ptnt_lkp AS 
  (
    SELECT --+ materialize
      fpd.network,
      fpd.patient_id,
      fpd.onset_date,
      fpd.diag_code htn_dx_code,
      ROW_NUMBER() OVER (PARTITION BY fpd.patient_id ORDER BY fpd.onset_date DESC) htn_ptnt_rnum   
    FROM dt
    JOIN FACT_PATIENT_DIAGNOSES fpd on  fpd.onset_date >= dt.msrmnt_yr_start_dt AND fpd.onset_date < ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1) AND fpd.network='CBN'
    JOIN pt005.meta_conditions mc
      ON mc.value=fpd.diag_code AND mc.criterion_id=36 AND include_exclude_ind='I'
    WHERE NOT EXISTS
      (
        SELECT 
          distinct fpd.patient_id
        FROM dt
        JOIN FACT_PATIENT_DIAGNOSES fpd1 on  fpd1.onset_date >= dt.msrmnt_yr_start_dt AND fpd1.onset_date < ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1) AND fpd1.network='CBN'
        JOIN pt005.meta_conditions mc ON mc.value=fpd1.diag_code AND mc.criterion_id=36 AND include_exclude_ind='E'  
        WHERE fpd1.patient_id=fpd.patient_id     
      )
  ),
--  select * from htn_ptnt_lkp;
  htn_metadata_rslts_lkp AS 
  (
    SELECT --+ materialize
      mc.VALUE,
      mc.value_description,
      CASE
         WHEN UPPER (mc.value_description) LIKE '%SYS%' THEN 'S' -- systolic
         WHEN UPPER (mc.value_description) LIKE '%DIAS%' THEN 'D' -- diastolic
         ELSE 'C' -- combo
      END test_type
    FROM pt005.meta_conditions mc
    WHERE mc.criterion_id = 13 AND mc.include_exclude_ind = 'I'
   ),
--   select * from htn_metadata_rslts_lkp;
   
   
   
   
   
   
   htn_op_visits AS 
  (
    SELECT --+ materialize ordered use_hash(v vt vsvl ld vsp fc)
      dt.begin_dt report_period_start_dt,
--      ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1) report_period_end_dt,
      dt.network,
      v.visit_id,
      v.final_visit_type_id,
--      name AS visit_type_name,
      v.financial_class_id,
--      fc.name AS visit_financial_class,               
      v.first_payer_key,
--      vsp.payer_id,
      v.facility_key,
--      v.facility_id,
      v.admission_dt,
      v.discharge_dt,
      v.patient_id,
      lkp.onset_date,
      lkp.htn_dx_code,
      row_number() over (partition by v.patient_id order by v.admission_date_time desc) rnum_ltst_visit
    FROM dt
    JOIN fact_visits v
      ON v.network=dt.network
--     AND v.admission_dt >= dt.begin_dt
--     AND v.admission_dt < ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1) 
    JOIN htn_ptnt_lkp lkp
      ON lkp.patient_id=v.patient_id 
     AND lkp.network=v.network
     AND lkp.htn_ptnt_rnum=1 

--      where v.patient_id=1412983
  )
  select * from htn_op_visits;
  
  
  
  
  
  
  
  
  
  
  rslt AS 
  (
    SELECT --+ use_hash(r evnt v)
      v.report_period_start_dt,
      v.report_period_end_dt,
      lkp.network,
      v.facility_key,
      v.patient_id,
      v.visit_id,
      v.final_visit_type_id,
--      ld.clinic_code,
--      cc.service clinic_code_service,
--      cc.description clinic_code_desc,
      v.financial_class_id,
--      v.visit_financial_class,               
--      v.payer_id,
      v.first_payer_key,
      v.admission_dt,
      v.discharge_dt,
      r.event_id,
      r.result_dt,
      r.data_element_id,
      lkp.value_description,
      r.result_value,
      CASE
        WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',1))
        WHEN lkp.test_type = 'S' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
      END AS systolic_bp,
      CASE
        WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',2))
        WHEN lkp.test_type = 'D' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
      END AS diastolic_bp   
    FROM htn_metadata_rslts_lkp lkp
    JOIN fact_results r
      ON r.data_element_id = lkp.value 
     AND r.network=lkp.network 
     AND cid >= 20150801000000
     AND r.result_dt >= lkp.begin_dt
     AND r.result_dt < lkp.end_dt
    JOIN htn_op_visits v
      ON v.visit_id = r.visit_id
     AND v.network = r.network
  )
    select * from rslt;
    
    
    select * from all_tab_columns where column_name='CLINIC_CODE' and owner='CDW';