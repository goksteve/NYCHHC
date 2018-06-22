CREATE OR REPLACE VIEW v_dsrip_report_tr018
AS
WITH 
  -- 22-JUN-2018, GK: script using star schema tables.
  dt AS
  (
    SELECT --+ materialize
      NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
      NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'YEAR')) msrmnt_yr,
      ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')), -24) begin_dt
    FROM dual
  ),
  htn_ptnt_lkp AS 
  (
    SELECT --+ materialize
      fpd.network,
      fpd.patient_id,
      fpd.onset_date,
      fpd.diag_code htn_diag_code,
      ROW_NUMBER() OVER (PARTITION BY fpd.patient_id ORDER BY fpd.onset_date DESC) htn_ptnt_rnum   
    FROM dt
    JOIN cdw.fact_patient_diagnoses fpd on  fpd.onset_date >= dt.msrmnt_yr 
     AND fpd.onset_date <  
          CASE 
            WHEN TO_CHAR(TRUNC(SYSDATE,'MONTH'),'mm/dd') < '06/01'
            THEN TRUNC(SYSDATE,'MONTH')
            ELSE TO_DATE('07/01','MM/DD')
          END    
    JOIN meta_conditions mc
      ON mc.value=fpd.diag_code AND mc.criterion_id=36 AND include_exclude_ind='I'
    WHERE NOT EXISTS
    (
      SELECT 
        distinct fpd1.patient_id,fpd1.network
      FROM dt
      JOIN fact_patient_diagnoses fpd1 on  fpd1.onset_date >= dt.msrmnt_yr 
      AND fpd1.onset_date <  
        CASE 
          WHEN TO_CHAR(TRUNC(SYSDATE,'MONTH'),'mm/dd') < '06/01'
          THEN TRUNC(SYSDATE,'MONTH')
          ELSE TO_DATE('07/01','MM/DD')
        END  
      JOIN meta_conditions mc ON mc.value=fpd1.diag_code AND mc.criterion_id=36 AND include_exclude_ind='E'  
      WHERE fpd1.patient_id=fpd.patient_id AND fpd1.network=fpd.network    
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
    FROM meta_conditions mc
    WHERE mc.criterion_id = 13 AND mc.include_exclude_ind = 'I'
   ),
  htn_op_visits AS 
  (
    SELECT --+ materialize 
      dt.report_dt, dt.begin_dt AS begin_dt, dt.report_dt AS end_dt, v.network, v.visit_id,v.final_visit_type_id,
      v.first_payer_key,v.facility_key, v.admission_dt, v.discharge_dt, v.patient_id, lkp.onset_date,
      lkp.htn_diag_code, v.first_department_key,
      row_number() over (partition by v.patient_id,v.network order by v.admission_dt desc) rnum_ltst_visit
    FROM dt
    JOIN cdw.fact_visits v
      ON v.admission_dt >= dt.begin_dt
     AND v.admission_dt < dt.report_dt
     AND v.final_visit_type_id IN (3,4) 
    JOIN htn_ptnt_lkp lkp
      ON lkp.patient_id=v.patient_id AND lkp.network=v.network
     AND lkp.htn_ptnt_rnum=1
  ),
  rslt AS 
  (
    SELECT -- use_hash(r evnt v)
      v.report_dt, v.begin_dt, v.end_dt, v.network, v.facility_key, v.patient_id,
      v.visit_id, v.final_visit_type_id, v.first_payer_key, v.admission_dt, v.discharge_dt, 
      r.event_id, r.result_dt, r.data_element_id,
      lkp.value_description, r.result_value,
      CASE
        WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',1))
        WHEN lkp.test_type = 'S' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
      END AS systolic_bp,
      CASE
        WHEN lkp.test_type = 'C' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})/([0-9]{2,})',1,1,'x',2))
        WHEN lkp.test_type = 'D' THEN TO_NUMBER (REGEXP_SUBSTR (r.result_value,'^[^0-9]*([0-9]{2,})',1,1,'',1))
      END AS diastolic_bp   
    FROM dt 
    JOIN cdw.fact_results r
      ON r.result_dt >= dt.begin_dt
     AND r.result_dt < dt.report_dt
    JOIN htn_metadata_rslts_lkp lkp 
      ON lkp.value = r.data_element_id 
     AND lkp.network = r.network 
     AND r.cid >= 20150801000000
    JOIN htn_op_visits v
      ON v.visit_id = r.visit_id
     AND v.network = r.network
	),
	rslt_combo AS 
	(
		SELECT 
      report_dt, g.begin_dt, g.end_dt, g.network, g.facility_key, g.patient_id,
      g.visit_id, g.final_visit_type_id, g.admission_dt, g.discharge_dt, g.first_payer_key,
      g.result_dt, event_id,g.systolic_bp, g.diastolic_bp, flag_140_90, flag_150_90,
      row_number() over (partition by g.network,g.patient_id order by flag_150_90,flag_140_90,result_dt desc) rnum_per_patient  
    FROM 
    (  
      SELECT   
        report_dt, 
        begin_dt, 
        end_dt, 
        network, 
        facility_key, 
        patient_id,
        visit_id, 
        final_visit_type_id, 
        first_payer_key,
        admission_dt, 
        discharge_dt, 
        event_id,
        result_dt,
        ROW_NUMBER() OVER (PARTITION BY network,patient_id,TRUNC(result_dt) ORDER BY result_dt DESC) rnum_per_day,
        MAX (systolic_bp) systolic_bp, MAX (diastolic_bp) diastolic_bp,
        CASE 
          WHEN MAX (systolic_bp) >140 AND MAX (diastolic_bp) >90 THEN 1 
          ELSE 0
        END flag_140_90,
        CASE 
          WHEN MAX (systolic_bp) >150 AND MAX (diastolic_bp) >90 THEN 1 
          ELSE 0
        END flag_150_90          
      FROM rslt
      GROUP BY report_dt, begin_dt, end_dt, network, facility_key, patient_id, visit_id, final_visit_type_id, 
      first_payer_key,admission_dt, discharge_dt, result_dt, event_id,result_dt
      HAVING MAX (systolic_bp) BETWEEN 0 AND 311 AND MAX (diastolic_bp) BETWEEN 0 AND 284
    ) g
    WHERE g.rnum_per_day = 1
  ),
  Denominator AS
  (	
	SELECT --+ parallel(32)
		v.report_dt, 
		v.begin_dt, 
		v.end_dt,
		v.network, 
		CASE
			WHEN r.patient_id IS NOT NULL
			THEN r.facility_key
			ELSE v.facility_key
		END facility_key, 
		v.patient_id,
		CASE
			WHEN r.patient_id IS NOT NULL
			THEN r.visit_id
			ELSE v.visit_id
		END visit_id,   
		CASE
			WHEN r.patient_id IS NOT NULL
			THEN r.final_visit_type_id
			ELSE v.final_visit_type_id
		END visit_type_id, 
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
			THEN r.first_payer_key
			ELSE v.first_payer_key
		END payer_id,
		v.onset_date htn_onset_date,
		v.htn_diag_code,
		r.result_dt, 
		r.event_id, 
		r.systolic_bp, 
		r.diastolic_bp, 
		r.flag_140_90, 
		r.flag_150_90, 
		r.rnum_per_patient,
		v.first_department_key,
		v.rnum_ltst_visit
	FROM htn_op_visits v 
	LEFT JOIN rslt_combo r 
	ON v.patient_id=r.patient_id 
	AND r.result_dt >= v.onset_date
	AND r.network = v.network 
	AND rnum_per_patient =1
	WHERE v.rnum_ltst_visit=1
	)
SELECT --+ parallel(32)
	d.report_dt, 
	d.network, 
	f.facility_name, 
	d.patient_id,
	p.name AS patient_name, 
	p.medical_record_number AS mrn,
	p.birthdate,
	p.apt_suite, 
	p.street_address, 
	p.city, 
	p.state, 
	p.country,
	p.mailing_code, 
	p.home_phone,
	p.cell_phone,
	FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) AS age, 
	vt.name visit_type_name, 
	d.visit_id, 
  dept.specialty_code clinic_code,
  dept.service clinic_code_service,	
  dept.specialty clinic_code_desc,
	trunc(d.admission_dt) admission_dt,
	trunc(d.discharge_dt) discharge_dt, 
	dp.payer_group, 
  dp.payer_name,
	CASE 
		WHEN FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - p.birthdate)/365) BETWEEN 18 AND 59 
		THEN 'Y' 
		ELSE 'N' 
	END AGE_18_59,
	CASE 
		WHEN FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - p.birthdate)/365) BETWEEN 60 AND 85 
		THEN 'Y' 
		ELSE 'N' 
	END AGE_60_85,
	CASE
		WHEN diab_prob_pat.patient_id IS NOT NULL
		THEN 'Y'
		ELSE 'N'
	END diabetic, 
	diab_prob_pat.diag_code diabetes_diag_code,
  diab_prob_pat.onset_date diabetes_onset_date,
  d.htn_diag_code,
  d.htn_onset_date,
  trunc(d.result_dt) bp_reading_time, 
	d.systolic_bp, 
	d.diastolic_bp, 
	CASE 
		WHEN ((FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 18 AND 59)AND (systolic_bp < 140 AND diastolic_bp <90))
		THEN 'Y'
		ELSE 'N'
	END numerator_flag1,
	CASE 
		WHEN FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 60 AND 85 AND (systolic_bp < 140 AND diastolic_bp <90) AND diab_prob_pat.patient_id IS NOT NULL
		THEN 'Y'
		ELSE 'N'
	END numerator_flag2,
	CASE 
		WHEN FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 60 AND 85 AND (systolic_bp < 150 AND diastolic_bp <90) AND diab_prob_pat.patient_id IS NULL
		THEN 'Y'
		ELSE 'N'
	END numerator_flag3
FROM denominator d
JOIN cdw.dim_patients p
  ON p.patient_id = d.patient_id AND p.network = d.network AND p.current_flag=1 AND p.date_of_death IS NULL
LEFT JOIN 
(
  SELECT 
    g.patient_id, g.network,MAX(g.onset_date) onset_date,MAX(g.diag_code) diag_code
  FROM  
  (
    SELECT --+ materialize
      fpd.network,fpd.patient_id,fpd.onset_date,fpd.diag_code
    FROM cdw.fact_patient_diagnoses fpd  
    JOIN meta_conditions mc
      ON mc.value=fpd.diag_code AND mc.criterion_id=37 AND include_exclude_ind='I'
    WHERE 
    NOT EXISTS
    (
      SELECT 
        distinct fpd1.patient_id,fpd1.network
      FROM cdw.fact_patient_diagnoses fpd1  
      JOIN meta_conditions mc ON mc.value=fpd1.diag_code AND mc.criterion_id=37 AND include_exclude_ind='E'  
      WHERE fpd1.patient_id=fpd.patient_id AND fpd1.network=fpd.network    
    )       
    
    UNION
    
    SELECT 
      DISTINCT network,a.patient_id,null onset_date,null diag_code
    FROM cdw.fact_patient_prescriptions a
    JOIN cdw.ref_drug_descriptions b
      ON a.drug_description = b.drug_description
      AND b.drug_type_id = 33
  )g
  GROUP BY g.network,g.patient_id 
)diab_prob_pat
ON diab_prob_pat.patient_id = p.patient_id 
AND diab_prob_pat.network = p.network
JOIN cdw.dim_hc_facilities f
  ON d.facility_key = f.facility_key
JOIN cdw.ref_visit_types vt
  ON vt.visit_type_id=d.visit_type_id
LEFT JOIN cdw.dim_payers dp
  ON dp.payer_key=d.payer_id
LEFT JOIN cdw.dim_hc_departments dept
  ON dept.department_key=d.first_department_key
WHERE
FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) BETWEEN 18 and 85;
