CREATE OR REPLACE VIEW V_DSRIP_REPORT_TR010
AS
WITH 
  -- 20-JUN-2018, GK: script using star schema tables.
  dt AS
  (
    SELECT --+ materialize
      NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
--      NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'YEAR')) msrmnt_yr,
      CASE
        WHEN TO_CHAR(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')),'MON') = 'JAN'
        THEN TRUNC(ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')), -1), 'YEAR') 
        ELSE TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')), 'YEAR')
      END AS msrmnt_yr,
      ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')), -24) begin_dt
    FROM dual
  ),

--temp table to locate patient visits with a principal diagnosis of asthma with in the measurement period
  asthma_ptnt_lkp AS 
  (
    SELECT --+ materialize
      a.network,
      a.report_dt,
      a.begin_dt,
      a.end_dt,  
      a.patient_id,
      a.visit_id,
      a.facility_key,
      a.facility_name,
      a.facility_cd,
      a.final_visit_type_id,
      a.final_visit_type,
      a.initial_visit_type_id,
      a.initial_visit_type,
      a.admission_dt,
      a.discharge_dt,
      a.icd_code,
      a.problem_description,
      a.coding_scheme,
      a.first_payer_key,
      a.financial_class_id,
      a.ptnt_prb_rnum,
      a.ptnt_vst_rnum,
      ROW_NUMBER() OVER(PARTITION BY a.network,a.patient_id ORDER BY a.admission_dt DESC) ltst_ptnt_rec
    FROM
    (       
      SELECT 
        DISTINCT vst.network,
        dt.report_dt,
        dt.begin_dt,
        dt.report_dt end_dt,
        vst_prb.patient_id,
        vst_prb.visit_id,
        vst.facility_key,
        fclty.facility_name,
        fclty.facility_cd,
        vst.final_visit_type_id,
        vt1.name final_visit_type,
        vst.initial_visit_type_id,
        vt2.name initial_visit_type,
        vst.admission_dt,
        vst.discharge_dt,
        vst_prb.icd_code,
        vst_prb.problem_comments problem_description,
        vst_prb.coding_scheme,
        vst.first_payer_key,
        vst.financial_class_id,
        ROW_NUMBER() OVER (PARTITION BY vst_prb.network, vst_prb.patient_id ORDER BY vst.admission_dt DESC,vst_prb.coding_scheme ASC) ptnt_prb_rnum,
        ROW_NUMBER() OVER (PARTITION BY vst_prb.network,vst_prb.patient_id,vst.visit_id ORDER BY vst.admission_dt DESC,vst_prb.coding_scheme ASC) ptnt_vst_rnum
      FROM dt 
      JOIN cdw.fact_visits /*ud_master.visit*/ vst
        ON vst.admission_dt BETWEEN dt.begin_dt AND dt.report_dt --AND vst.patient_id = 1572634 AND vst.network = 'CBN'
      JOIN cdw.fact_visit_diagnoses/*goreliks1.visit_event_icd_code*/ vst_prb --TABLE WITH VISIT AND DIAGNOSIS RELATIONSHIP
        ON vst_prb.visit_id = vst.visit_id AND vst_prb.network = vst.network
      JOIN meta_conditions metac -- ASTHMA DIAGNOSIS CODE METADATA
        ON metac.value = vst_prb.icd_code AND metac.criterion_id = 21 AND INCLUDE_EXCLUDE_IND = 'I' AND metac.NETWORK='ALL'
      LEFT JOIN cdw.ref_visit_types vt1
        ON vt1.visit_type_id = vst.final_visit_type_id
      LEFT JOIN cdw.dim_hc_facilities fclty
        ON fclty.facility_key = vst.facility_key
      LEFT JOIN cdw.ref_visit_types vt2
        ON vt2.visit_type_id = vst.initial_visit_type_id        
       AND
       NOT EXISTS
      (
        SELECT 
          cmv.patient_id,cmv.network
        FROM cdw.fact_patient_diagnoses cmv
        JOIN meta_conditions metac1
          ON metac1.value = cmv.diag_code
        WHERE metac1.criterion_id = 21
         AND metac1.INCLUDE_EXCLUDE_IND = 'E' 
         AND metac.NETWORK='ALL'
         AND cmv.patient_id = vst_prb.patient_id
         AND cmv.network = vst_prb.network
      )
    ) a
  ),
  pcp_vst_dt AS
  (
    SELECT --+ materialize
      vst_lkp.network,
      vst_lkp.patient_id,
      vst_lkp.visit_id,
      vst_lkp.admission_dt,
      ROW_NUMBER() OVER (PARTITION BY vst_lkp.network,vst_lkp.patient_id ORDER BY vst_lkp.admission_dt DESC) pcp_visit_dt_rnum
    FROM asthma_ptnt_lkp vst_lkp
    JOIN cdw.fact_visits vst 
      ON vst.patient_id = vst_lkp.patient_id AND vst.network = vst_lkp.network --AND vst.patient_id = 1572634 AND vst.network = 'CBN'
    JOIN cdw.dim_hc_departments pcp
      ON pcp.department_key = vst.last_department_key AND pcp.service_type = 'PCP'         
  ),
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
            ROW_NUMBER() OVER (PARTITION BY network,patient_id,admission_dt ORDER BY NULL) RN
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
    SELECT --+ materialize
      network,
      report_dt,
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
          dt.report_dt,
          dt.begin_dt,
          dt.report_dt as end_dt
        FROM dt
        JOIN cdw.fact_patient_prescriptions rx
        ON rx.order_dt between dt.begin_dt and dt.report_dt --AND rx.patient_id = 1572634 AND rx.network = 'CBN'
        JOIN cdw.ref_drug_descriptions rd
        ON rx.drug_description=rd.drug_description
        AND rd.drug_type_id IN (40,41)  
      )
  ),  
--temp table to retrieve asthma controller medication prescriptions for the identified patients using metadata provided by BA
  asthma_cntrlr_med_dspns_evnt AS
  (
    SELECT --+ materialize
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
        JOIN cdw.fact_patient_prescriptions rx
        ON rx.order_dt between dt.begin_dt and dt.report_dt --AND rx.patient_id = 1572634 AND rx.network = 'CBN'
        JOIN cdw.ref_drug_descriptions rd
          ON rx.drug_description=rd.drug_description
         AND rd.drug_type_id=42   
      )
  ),  
--temp table to retrieve asthma other medication prescriptions for the identified patients using metadata provided by BA
  asthma_other_med_dspns_evnt AS
  (
    SELECT --+ materialize
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
        JOIN cdw.fact_patient_prescriptions rx
        ON rx.order_dt between dt.begin_dt and dt.report_dt --AND rx.patient_id = 1572634 AND rx.network = 'CBN'
        JOIN cdw.ref_drug_descriptions rd
          ON rx.drug_description=rd.drug_description
         AND rd.drug_type_id=43  
      )
  )
SELECT 
  DISTINCT
  a.report_dt,
  a.begin_dt,
  a.end_dt,
  a.network,
  a.facility_name,
  a.patient_id,
  a.visit_id,	
  ptnt.name AS patient_name,
  nvl(stg.second_mrn, ptnt.medical_record_number) AS mrn,
  ptnt.apt_suite,		
  ptnt.street_address,	
  ptnt.city,			
  ptnt.state,			
  ptnt.country,			
  ptnt.mailing_code,	
  ptnt.home_phone,
  ptnt.cell_phone,
  ptnt.birthdate, 
  FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - ptnt.birthdate)/365) AS age,
  ptnt.pcp_provider_id AS pcp_id,
  ptnt.pcp_provider_name pcp_name,
  f.visit_id last_pcp_visit_id,
  f.admission_dt last_pcp_visit_dt,
  a.final_visit_type,
  a.initial_visit_type,
  a.admission_dt,
  a.discharge_dt,
  pm.payer_name,
  pm.payer_group,  
  fin.financial_class_name plan_name,
  CASE 
    WHEN UPPER(pm.payer_group) LIKE '%MEDICAID%' THEN 'Y' 
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
JOIN cdw.dim_patients ptnt
  ON ptnt.patient_id = a.patient_id
 AND ptnt.network = a.network 
 AND ptnt.current_flag=1  AND ptnt.date_of_death IS NULL
 AND (FLOOR((ADD_MONTHS(TRUNC(SYSDATE,'year'),12)-1 - ptnt.birthdate)/365) BETWEEN 5 AND 64)  
LEFT JOIN cdw.dim_payers pm
  ON pm.payer_id=a.first_payer_key AND pm.network=a.network  
LEFT JOIN cdw.ref_financial_class fin 
  ON fin.financial_class_id = a.financial_class_id AND fin.network = a.network
LEFT JOIN cdw.ref_patient_secondary_mrn stg
  ON stg.network = a.network AND stg.patient_id = a.patient_id AND stg.facility_key = a.facility_key
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
  ); 