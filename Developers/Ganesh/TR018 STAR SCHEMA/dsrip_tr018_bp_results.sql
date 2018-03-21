WITH 
  dt AS 
  (
    SELECT --+ materialize
      TRUNC(SYSDATE, 'MONTH') AS report_period_start_dt,
      TRUNC(SYSDATE, 'YEAR') AS msrmnt_yr_start_dt,
      ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) begin_dt,
      TRUNC (ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1), 'MONTH') end_dt
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
    JOIN fact_patient_diagnoses fpd on  fpd.onset_date >= dt.msrmnt_yr_start_dt AND fpd.onset_date  < ADD_MONTHS(msrmnt_yr_start_dt,1)  AND fpd.network='CBN'
    JOIN pt005.meta_conditions mc
      ON mc.value=fpd.diag_code AND mc.criterion_id=36 AND include_exclude_ind='I'
    WHERE NOT EXISTS
      (
        SELECT 
          distinct fpd.patient_id
        FROM dt
        JOIN FACT_PATIENT_DIAGNOSES fpd1 on  fpd1.onset_date >= dt.msrmnt_yr_start_dt AND fpd1.onset_date  < ADD_MONTHS(msrmnt_yr_start_dt,1)  AND fpd1.network='CBN'
        JOIN pt005.meta_conditions mc ON mc.value=fpd1.diag_code AND mc.criterion_id=36 AND include_exclude_ind='E'  
        WHERE fpd1.patient_id=fpd.patient_id     
      )
  ),
  htn_metadata_rslts_lkp AS 
  (
    SELECT --+ materialize
      mc.network,
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
  htn_op_visits AS 
  (
    SELECT --+ materialize 
      dt.begin_dt report_period_start_dt,
      dt.end_dt report_period_end_dt,
      v.network,
      v.visit_id,
      rvt.name visit_type,
      v.final_visit_type_id,
      rvt.name AS visit_type_name,
      v.financial_class_id,
      fin.financial_class_name AS visit_financial_class,               
      dp.payer_id,
      dp.payer_name,
      dp.payer_group,
      fclty.facility_name,
      v.admission_dt,
      v.discharge_dt,
      v.patient_id,
      lkp.onset_date,
      lkp.htn_dx_code,
      v.last_department_key,
      row_number() over (partition by v.patient_id,v.network order by v.admission_dt desc) rnum_ltst_visit
    FROM dt
    JOIN fact_visits v
      ON v.admission_dt >= dt.begin_dt
     AND v.admission_dt < dt.end_dt
    JOIN htn_ptnt_lkp lkp
      ON lkp.patient_id=v.patient_id 
     AND lkp.htn_ptnt_rnum=1
    JOIN ref_visit_types rvt
      ON rvt.visit_type_id=v.final_visit_type_id
     AND rvt.visit_type_id IN (3,4)  
    LEFT JOIN dim_hc_facilities fclty
      ON fclty.facility_key=v.facility_key 
    LEFT JOIN ref_financial_class fin
      ON fin.network=v.network
     AND fin.financial_class_id=v.financial_class_id
    LEFT JOIN dim_payers dp
      ON dp.payer_key=v.first_payer_key
  ),
  rslt AS 
  (
    SELECT -- use_hash(r evnt v)
      v.report_period_start_dt,
      v.report_period_end_dt,
      v.network,
      v.facility_name,
      v.patient_id,
      v.visit_id,
      v.visit_type,
      v.financial_class_id,
      v.visit_financial_class,               
      v.payer_id,
      v.admission_dt,
      v.discharge_dt,
      r.event_id,
      r.result_dt,
      r.data_element_id,
      lkp.value_description,
      r.result_value,
      dept.service clinic_code_service,
      dept.specialty clinic_code_desc,
      dept.specialty_code clinic_code,
      CASE
        WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',1))
        WHEN lkp.test_type = 'S' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
      END AS systolic_bp,
      CASE
        WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',2))
        WHEN lkp.test_type = 'D' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
      END AS diastolic_bp   
    FROM dt 
    JOIN fact_results r
      ON r.result_dt >= dt.begin_dt
     AND r.result_dt < dt.end_dt
    JOIN htn_metadata_rslts_lkp lkp 
      ON lkp.value = r.data_element_id 
     AND lkp.network = r.network 
     AND r.cid >= 20150801000000
    JOIN htn_op_visits v
      ON v.visit_id = r.visit_id
     AND v.network = r.network
    LEFT JOIN DIM_HC_DEPARTMENTS dept
      ON dept.department_key=v.last_department_key
     AND dept.service_type='PCP' 
  ),
rslt_combo AS 
  (
    SELECT 
      g.report_period_start_dt,
      g.report_period_end_dt,
      g.network,
      g.facility_name,
      g.patient_id,
      g.visit_id,
      g.visit_type,
      g.admission_dt,
      g.discharge_dt,
      g.clinic_code,
      g.clinic_code_service,
      g.clinic_code_desc,
      g.financial_class_id,
      g.visit_financial_class,               
      g.payer_id,
      g.result_dt,
      g.event_id,
      g.systolic_bp,
      g.diastolic_bp,
      flag_140_90,
      flag_150_90,
      row_number() over (partition by g.patient_id order by flag_150_90,flag_140_90,result_dt desc) rnum_per_patient  
    FROM 
    (  
      SELECT   
        report_period_start_dt,
        report_period_end_dt,
        network,
        facility_name,
        patient_id,
        visit_id,
        visit_type,
        clinic_code,
        clinic_code_service,
        clinic_code_desc,
        financial_class_id,
        visit_financial_class,               
        payer_id,
        admission_dt,
        discharge_dt,
        event_id,
        result_dt,
        ROW_NUMBER() OVER (PARTITION BY patient_id,network,TRUNC(result_dt) ORDER BY result_dt DESC) rnum_per_day,
        MAX (systolic_bp) systolic_bp,
        MAX (diastolic_bp) diastolic_bp,
        CASE 
          WHEN MAX (systolic_bp) >140 AND MAX (diastolic_bp) >90 THEN 1 
          ELSE 0
        END flag_140_90,
        CASE 
          WHEN MAX (systolic_bp) >150 AND MAX (diastolic_bp) >90 THEN 1 
          ELSE 0
        END flag_150_90          
      FROM rslt
      GROUP BY report_period_start_dt, report_period_end_dt, network, facility_name, patient_id, visit_id, visit_type, financial_class_id, visit_financial_class, payer_id,                         
              admission_dt, discharge_dt, result_dt, event_id,clinic_code,clinic_code_desc,clinic_code_service
      HAVING MAX (systolic_bp) BETWEEN 0 AND 311 AND MAX (diastolic_bp) BETWEEN 0 AND 284
    ) g
    WHERE g.rnum_per_day = 1
  )
SELECT --+ parallel(32)
  v.report_period_start_dt, 
  v.report_period_end_dt,
  v.network, 
  v.facility_name, 
  r.clinic_code,
  r.clinic_code_service,
  r.clinic_code_desc,
  v.patient_id,
  CASE
    WHEN r.patient_id IS NOT NULL
    THEN r.visit_id
    ELSE v.visit_id
  END visit_id,   
  CASE
    WHEN r.patient_id IS NOT NULL
    THEN r.visit_type
    ELSE v.visit_type
  END visit_type, 
  CASE
    WHEN r.patient_id IS NOT NULL
    THEN r.admission_dt
    ELSE v.admission_dt 
  END admission_dt,
  CASE
    WHEN r.patient_id IS NOT NULL
    THEN r.discharge_dt
    ELSE v.discharge_dt
  END discharge_dt, 
  CASE
    WHEN r.patient_id IS NOT NULL
    THEN r.financial_class_id
    ELSE v.financial_class_id
  END financial_class_id,   
  CASE
    WHEN r.patient_id IS NOT NULL
    THEN r.visit_financial_class
    ELSE v.visit_financial_class
  END visit_financial_class,                 
  CASE
    WHEN r.patient_id IS NOT NULL
    THEN r.payer_id
    ELSE v.payer_id
  END payer_id,
  v.onset_date,
  v.htn_dx_code,
  r.result_dt, 
  r.event_id, 
  r.systolic_bp, 
  r.diastolic_bp, 
  r.flag_140_90, 
  r.flag_150_90, 
  r.rnum_per_patient
FROM htn_op_visits v 
LEFT JOIN rslt_combo r 
  ON v.patient_id=r.patient_id 
 AND r.result_dt >= v.onset_date 
 AND rnum_per_patient =1
WHERE v.rnum_ltst_visit=1
AND v.network='CBN';
