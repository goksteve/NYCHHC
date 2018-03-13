INSERT INTO dsrip_tr10_asthma_medratio_rpt
WITH 
  dt AS 
  (
    SELECT --+ materialize
      TRUNC(SYSDATE, 'MONTH') AS report_run_dt,
      TRUNC(SYSDATE, 'YEAR') AS msrmnt_yr,
      ADD_MONTHS (TRUNC (SYSDATE, 'MONTH'), -24) begin_dt,
      TRUNC (SYSDATE, 'MONTH') end_dt
    FROM DUAL
  ),
  asthma_ptnt_lkp AS 
  (
    SELECT 
      network,	
      patient_id,	
      visit_id,	
      facility_id,
      facility_name,	
      latest_visit_type_id,	
      latest_visit_type,	
      initial_visit_type_id,	
      initial_visit_type,	
      admission_date_time,	
      discharge_date_time,	
      icd_code,	
      problem_description,	
      payer_id,
      financial_class_id,
      fin_plan_name,       
      ptnt_prb_rnum,	
      ptnt_vst_rnum,
      ROW_NUMBER() OVER(PARTITION BY NETWORK,patient_id ORDER BY admission_date_time DESC) ltst_ptnt_rec
    FROM tr010_stg_asthma_ptnt_vsts 
  ),
  pcp_vst_dt AS
  (
    SELECT --+ materialize
      vst_lkp.network,
      vst_lkp.patient_id,
      vst_lkp.visit_id,
      vst_lkp.admission_date_time,
      ROW_NUMBER() OVER (PARTITION BY vst_lkp.network,vst_lkp.patient_id ORDER BY vst_lkp.admission_date_time DESC) pcp_visit_dt_rnum
    FROM asthma_ptnt_lkp vst_lkp
    JOIN cdw.visit_segment_visit_location vsvl
      ON vsvl.visit_id = vst_lkp.visit_id   
     AND vsvl.network=vst_lkp.network
     AND vsvl.visit_segment_number=1
    JOIN cdw.hhc_location_dimension ld
      ON ld.location_id = vsvl.location_id
     AND ld.network=vsvl.network
    JOIN xx_pcp_codes pcp
      ON pcp.code = ld.clinic_code
     AND pcp.network=vst_lkp.network  
  ),
----temp table to count different visit types per patient, this is useful to determine persistent asthma
  ptnt_vst_type_cnt AS
  (
    SELECT --+ materialize
    *
    FROM 
    ( 
      (
        SELECT 
          network,
          patient_id, 
          initial_visit_type
        FROM 
        (
          SELECT 
            network,
            patient_id,
            initial_visit_type,
            ROW_NUMBER() OVER (PARTITION BY network,patient_id,admission_date_time ORDER BY NULL) RN
          FROM asthma_ptnt_lkp
        )
        WHERE rn = 1
      )
      PIVOT
      (
        COUNT(*) FOR initial_visit_type IN ('Clinic' clinic_visit_count, 'Emergency' emergency_visit_count,'Inpatient' inpatient_visit_count,'Outpatient' outpatient_visit_count)
      )
    )  
  ),
--temp table to retrieve leukotriene modifiers and antibody inhibitors prescriptions for the identified patients using metadata provided by BA
  med_luk_antbdy_dspns_evnt AS
  (
    SELECT 
      network,
      report_run_dt,
      begin_dt,
      end_dt,
      patient_id,
      COUNT(*) OVER (PARTITION BY network,patient_id) med_luk_antbdy_dspns_evnt_cnt,
      ROW_NUMBER() OVER (PARTITION BY network,patient_id ORDER BY rx_order_time DESC) ltst_luk_antbdy_rnum
    FROM  
      (
        SELECT 
          DISTINCT rx.network, 
          rx.patient_id,
          rx.order_dt rx_order_time,
          rx_exp_dt rx_exp_time,
          rx.drug_name derived_product_name,
          rx.rx_quantity,
          rx.rx_dc_dt rx_dc_time,
          dt.report_run_dt,
          dt.begin_dt,
          dt.end_dt
        FROM dt
        JOIN fact_prescriptions rx
        ON rx.order_dt between dt.begin_dt and dt.end_dt
        JOIN REF_DRUG_DESCRIPTIONS rd
          ON rx.drug_description=rd.drug_description
         AND rd.drug_type_id IN (40,41)   
      )
  ),
--temp table to retrieve asthma controller medication prescriptions for the identified patients using metadata provided by BA
  asthma_cntrlr_med_dspns_evnt AS
  (
    SELECT 
      network,
      patient_id,
      rx_order_time,
      derived_product_name,      
      COUNT(*) OVER (PARTITION BY network,patient_id) asthma_cntrlr_med_cnt,
      ROW_NUMBER() OVER (PARTITION BY network,patient_id ORDER BY rx_order_time DESC) ltst_cntrlr_med_rnum
    FROM  
      (
        SELECT 
          DISTINCT rx.network, 
          rx.patient_id,
          rx.order_dt rx_order_time,
          rx_exp_dt rx_exp_time,
          rx.drug_name derived_product_name,
          rx.rx_quantity,
          rx.rx_dc_dt rx_dc_time
        FROM dt
        JOIN fact_prescriptions rx
        ON rx.order_dt between dt.begin_dt and dt.end_dt
        JOIN REF_DRUG_DESCRIPTIONS rd
          ON rx.drug_description=rd.drug_description
         AND rd.drug_type_id=42   
      )
  ),
--temp table to retrieve asthma other medication prescriptions for the identified patients using metadata provided by BA
  asthma_other_med_dspns_evnt AS
  (
    SELECT 
      network,
      patient_id,
      rx_order_time,
      derived_product_name,      
      COUNT(*) OVER (PARTITION BY network,patient_id) asthma_other_med_cnt,
      ROW_NUMBER() OVER (PARTITION BY network,patient_id ORDER BY rx_order_time DESC) ltst_other_med_rnum
    FROM  
      (
        SELECT 
          DISTINCT rx.network, 
          rx.patient_id,
          rx.order_dt rx_order_time,
          rx_exp_dt rx_exp_time,
          rx.drug_name derived_product_name,
          rx.rx_quantity,
          rx.rx_dc_dt rx_dc_time
        FROM dt
        JOIN fact_prescriptions rx
        ON rx.order_dt between dt.begin_dt and dt.end_dt
        JOIN REF_DRUG_DESCRIPTIONS rd
          ON rx.drug_description=rd.drug_description
         AND rd.drug_type_id=43  
      )
  )
SELECT --+ parallel 
  DISTINCT
  c.report_run_dt,
  c.begin_dt,
  c.end_dt,
  a.network,
  a.facility_name,
  a.patient_id,
  a.visit_id,	
  ptnt.name patient_name,
  NVL(psn.medical_record_number, ptnt.medical_record_number) AS mrn,
  ptnt.apt_suite,		
  ptnt.street_address,	
  ptnt.city,			
  ptnt.state,			
  ptnt.country,			
  ptnt.mailing_code,	
  ptnt.home_phone,
  ptnt.birthdate, 
  FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - ptnt.birthdate)/365) age,
--  pcp.name(pt005 patient_dimension),
  ptnt.emp_provider_id,
  ptnt.emp_provider_name,
  f.visit_id last_pcp_visit_id,
  f.admission_date_time last_pcp_visit_dt,
  a.latest_visit_type,
  a.initial_visit_type,
  a.admission_date_time,
  a.discharge_date_time,
--  MEDICAID_IND(pt005 payer_dimension),
--  Payer Group(pt005 payer_dimension),
--  PAYER_NAME(pt005 payer_dimension),
--  PLAN_NAME(pt005 payer_dimension)
  pm.payer_name,
  pm.payer_group,
  a.fin_plan_name plan_name,
  CASE 
    WHEN UPPER(a.fin_plan_name) LIKE '%MEDICAID%' THEN 'Y' 
    ELSE 'N' 
  END AS medicaid_ind,
  a.icd_code,	
  a.problem_description,
  e.derived_product_name asthma_cntrlr_med_name,
  e.rx_order_time asthma_cntrlr_med_disp_date,
  d.rx_order_time asthma_other_med_disp_date,
  d.derived_product_name asthma_other_med_name, 
  e.asthma_cntrlr_med_cnt,
  d.asthma_other_med_cnt,
  c.med_luk_antbdy_dspns_evnt_cnt,
  (e.asthma_cntrlr_med_cnt+d.asthma_other_med_cnt) cnt_of_total_medications,
  round((e.asthma_cntrlr_med_cnt/(e.asthma_cntrlr_med_cnt+d.asthma_other_med_cnt)),2) ratio,  
--Numerator criteria
--Number of people within the denominator with a ratio of controller medications to total asthma medications of .50 or greater
  CASE 
    WHEN (e.asthma_cntrlr_med_cnt/(e.asthma_cntrlr_med_cnt+d.asthma_other_med_cnt)) >= 0.50
    THEN 'Y'
    ELSE 'N'
  END numerator_flag,
  b.clinic_visit_count,
  b.emergency_visit_count,
  b.inpatient_visit_count,
  b.outpatient_visit_count
FROM asthma_ptnt_lkp a
LEFT JOIN pcp_vst_dt f
  ON f.patient_id=a.patient_id
 AND f.network=a.network 
 AND pcp_visit_dt_rnum=1
JOIN ptnt_vst_type_cnt b 
  ON b.patient_id=a.patient_id and b.network=a.network
JOIN med_luk_antbdy_dspns_evnt c
  ON c.patient_id=a.patient_id 
 AND c.network=a.network 
JOIN asthma_cntrlr_med_dspns_evnt e
  ON e.patient_id=a.patient_id
 AND e.network=a.network  
 AND e.ltst_cntrlr_med_rnum=1   
JOIN asthma_other_med_dspns_evnt d
  ON d.patient_id=a.patient_id
 AND d.network=a.network  
 AND d.ltst_other_med_rnum=1  
JOIN patient_dimension ptnt
  ON ptnt.patient_id=a.patient_id
 AND ptnt.network=a.network 
 AND ptnt.current_flag=1  AND ptnt.date_of_death IS NULL
 AND (FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - ptnt.birthdate)/365) BETWEEN 5 AND 64)  
LEFT JOIN pt008.patient_secondary_stage psn
  ON psn.patient_id=a.patient_id AND psn.facility_id=a.facility_id AND psn.network=a.network and psn.visit_id=a.visit_id 
LEFT JOIN pt008.payer_mapping pm
  ON pm.payer_id=a.payer_id AND pm.network=a.network  
WHERE a.ptnt_prb_rnum=1
 AND
  (
    emergency_visit_count > 0
    OR
    inpatient_visit_count > 0
    OR
    ((outpatient_visit_count>=4 OR clinic_visit_count>=4) AND (e.asthma_cntrlr_med_cnt+d.asthma_other_med_cnt)>=2)
    OR
    (e.asthma_cntrlr_med_cnt+d.asthma_other_med_cnt)>=4
    OR
    med_luk_antbdy_dspns_evnt_cnt>=4
  )
  AND FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'YEAR'),12)-1 - ptnt.birthdate)/365)  BETWEEN 5 AND 64;