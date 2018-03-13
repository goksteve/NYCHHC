-- 03-13-2018 GK: Fix for diagnosis exclusion date conditions
WITH 
  dt AS 
  (
    SELECT --+ materialize
      SUBSTR(ORA_DATABASE_NAME, 1, 3) network,
      TRUNC(SYSDATE, 'MONTH') AS report_period_start_dt,
      TRUNC(SYSDATE, 'YEAR') AS msrmnt_yr_start_dt,
      ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) begin_dt,
      TRUNC (SYSDATE, 'MONTH') end_dt
    FROM DUAL
  ),
   htn_ptnt_lkp AS 
  (
    SELECT --+ materialize
      p.patient_id,
      p.onset_date,
      cmv.code htn_dx_code,
      ROW_NUMBER() OVER (PARTITION BY cmv.patient_id ORDER BY onset_date DESC) htn_ptnt_rnum   
    FROM dt
    JOIN ud_master.problem p 
      ON p.onset_date >= msrmnt_yr_start_dt AND p.onset_date < ADD_MONTHS(msrmnt_yr_start_dt,6) 
    JOIN ud_master.problem_cmv cmv 
      ON cmv.patient_id=p.patient_id  
    JOIN mdm_extract.meta_conditions mc
      ON mc.value=cmv.code AND mc.criterion_id=36 AND include_exclude_ind='I'
    WHERE NOT EXISTS
      (
        SELECT 
          distinct cmv1.patient_id
        FROM dt
        JOIN ud_master.problem p1 
          ON p1.onset_date >= msrmnt_yr_start_dt AND p1.onset_date < ADD_MONTHS(msrmnt_yr_start_dt,6) 
        JOIN ud_master.problem_cmv cmv1 
          ON cmv1.patient_id=p1.patient_id 
        JOIN mdm_extract.meta_conditions mc1
          ON mc1.value=cmv1.code AND mc1.criterion_id=36 AND mc1.include_exclude_ind='E'  
        WHERE cmv1.patient_id=p.patient_id     
      )
    ),
  htn_results_lkp AS 
  (
    SELECT --+ materialize
      dt.network,
      dt.begin_dt,
      dt.end_dt,
      mc.VALUE,
      mc.value_description,
      CASE
         WHEN UPPER (mc.value_description) LIKE '%SYS%' THEN 'S' -- systolic
         WHEN UPPER (mc.value_description) LIKE '%DIAS%' THEN 'D' -- diastolic
         ELSE 'C' -- combo
      END test_type
    FROM dt
    JOIN kollurug.meta_conditions mc
      ON mc.network = dt.network
     AND mc.criterion_id = 13
     AND mc.include_exclude_ind = 'I'
   ),
  htn_op_visits AS 
  (
    SELECT --+ materialize ordered use_hash(v vt vsvl ld vsp fc)
      dt.begin_dt report_period_start_dt,
      dt.end_dt report_period_end_dt,
      dt.network,
      v.visit_id,
      vt.name AS visit_type_name,
      v.financial_class_id,
      fc.name AS visit_financial_class,               
      vsp.payer_id,
      v.facility_id,
      v.admission_date_time,
      v.discharge_date_time,
      v.patient_id,
      lkp.onset_date,
      lkp.htn_dx_code,
      row_number() over (partition by v.patient_id order by v.admission_date_time desc) rnum_ltst_visit
    FROM dt
    JOIN ud_master.visit v
      ON v.admission_date_time >= dt.begin_dt
     AND v.admission_date_time < dt.end_dt
    JOIN htn_ptnt_lkp lkp
      ON lkp.patient_id=v.patient_id 
     AND lkp.htn_ptnt_rnum=1 
    JOIN ud_master.visit_type vt 
      ON vt.visit_type_id=v.visit_type_id 
     AND vt.visit_type_id IN (3,4) 
    LEFT JOIN ud_master.visit_segment_payer vsp
      ON vsp.visit_id = v.visit_id
     AND vsp.visit_segment_number = 1
     AND vsp.payer_number = 1
    LEFT JOIN ud_master.financial_class fc
      ON fc.financial_class_id = v.financial_class_id
--      where v.patient_id=1412983
  ),
  rslt AS 
  (
    SELECT --+ use_hash(r evnt v)
      v.report_period_start_dt,
      v.report_period_end_dt,
      lkp.network,
      v.facility_id,
      v.patient_id,
      v.visit_id,
      v.visit_type_name,
      ld.clinic_code,
      cc.service clinic_code_service,
      cc.description clinic_code_desc,
      v.financial_class_id,
      v.visit_financial_class,               
      v.payer_id,
      v.admission_date_time,
      v.discharge_date_time,
      evnt.event_id,
      evnt.date_time,
      r.data_element_id,
      lkp.value_description,
      r.VALUE,
      CASE
        WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.VALUE,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',1))
        WHEN lkp.test_type = 'S' THEN TO_NUMBER (REGEXP_SUBSTR (r.VALUE,'^[^0-9]*([0-9]{2,})',1,1,'',1))
      END AS systolic_bp,
      CASE
        WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.VALUE,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',2))
        WHEN lkp.test_type = 'D' THEN TO_NUMBER (REGEXP_SUBSTR (r.VALUE,'^[^0-9]*([0-9]{2,})',1,1,'',1))
      END AS diastolic_bp   
    FROM htn_results_lkp lkp
    JOIN ud_master.result r
      ON r.data_element_id = lkp.VALUE
     AND cid >= 20150801000000
    JOIN ud_master.event evnt
      ON evnt.visit_id = r.visit_id
     AND evnt.event_id = r.event_id
     AND evnt.date_time >= lkp.begin_dt
     AND evnt.date_time < lkp.end_dt
    JOIN htn_op_visits v
      ON v.visit_id = r.visit_id
    LEFT JOIN ud_master.visit_segment_visit_location vsvl
      ON vsvl.visit_id=v.visit_id 
     AND vsvl.visit_segment_number=1
    LEFT JOIN hhc_custom.hhc_location_dimension ld
      ON ld.location_id=vsvl.location_id  
    LEFT JOIN hhc_custom.hhc_clinic_codes cc
      ON cc.code=ld.clinic_code
      ),
  rslt_combo AS 
  (
    SELECT 
      g.report_period_start_dt,
      g.report_period_end_dt,
      g.network,
      g.facility_id,
      g.patient_id,
      g.visit_id,
      g.visit_type_name,
      g.admission_date_time,
      g.discharge_date_time,
      g.clinic_code,
      g.clinic_code_service,
      g.clinic_code_desc,
      g.financial_class_id,
      g.visit_financial_class,               
      g.payer_id,
      g.date_time,
      g.event_id,
      g.systolic_bp,
      g.diastolic_bp,
      flag_140_90,
      flag_150_90,
      row_number() over (partition by g.patient_id order by flag_150_90,flag_140_90,date_time desc) rnum_per_patient  
    FROM 
    (  
      SELECT   
        report_period_start_dt,
        report_period_end_dt,
        network,
        facility_id,
        patient_id,
        visit_id,
        visit_type_name,
        clinic_code,
        clinic_code_service,
        clinic_code_desc,
        financial_class_id,
        visit_financial_class,               
        payer_id,
        admission_date_time,
        discharge_date_time,
        event_id,
        date_time,
        ROW_NUMBER() OVER (PARTITION BY patient_id, TRUNC (date_time) ORDER BY date_time DESC) rnum_per_day,
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
      GROUP BY report_period_start_dt, report_period_end_dt, network, facility_id, patient_id, visit_id, visit_type_name, financial_class_id, visit_financial_class, payer_id,                         
              admission_date_time, discharge_date_time, date_time, event_id,clinic_code,clinic_code_desc,clinic_code_service
      HAVING MAX (systolic_bp) BETWEEN 0 AND 311 AND MAX (diastolic_bp) BETWEEN 0 AND 284
    ) g
    WHERE g.rnum_per_day = 1
  )
SELECT --+ parallel
  v.report_period_start_dt, 
  v.report_period_end_dt, 
  v.network, 
  v.facility_id, 
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
    THEN r.visit_type_name
    ELSE v.visit_type_name
  END visit_type_name, 
  CASE
    WHEN r.patient_id IS NOT NULL
    THEN r.admission_date_time
    ELSE v.admission_date_time 
  END admission_date_time,
  CASE
    WHEN r.patient_id IS NOT NULL
    THEN r.discharge_date_time
    ELSE v.discharge_date_time
  END discharge_date_time, 
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
  r.date_time, 
  r.event_id, 
  r.systolic_bp, 
  r.diastolic_bp, 
  r.flag_140_90, 
  r.flag_150_90, 
  r.rnum_per_patient
FROM htn_op_visits v 
LEFT JOIN rslt_combo r 
  ON v.patient_id=r.patient_id 
 AND r.date_time >= v.onset_date 
 AND rnum_per_patient =1
WHERE v.rnum_ltst_visit=1;
