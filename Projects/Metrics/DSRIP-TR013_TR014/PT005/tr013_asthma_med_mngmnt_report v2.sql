DROP TABLE dsrip_tr13_asthma_med_mgt_rpt PURGE;

CREATE TABLE dsrip_tr13_asthma_med_mgt_rpt
AS
WITH 
  dt AS 
  (
    SELECT --+ materialize
      TRUNC(SYSDATE, 'MONTH') AS report_run_dt,
      TRUNC(SYSDATE, 'YEAR') AS msrmnt_yr,
      ADD_MONTHS (TRUNC (SYSDATE, 'MONTH'), -24) begin_dt,
      TRUNC (SYSDATE, 'MONTH') -1 end_dt
    FROM DUAL
  ),
  asthma_ptnt_lkp AS 
  (
    SELECT 
      network,	
      ADD_MONTHS(TRUNC (ADD_MONTHS(SYSDATE,-1) ,'YEAR'),12)-1 msrmnt_yr_end_dt,
      report_run_dt,
      begin_dt,
      end_dt,	      
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
    FROM tr013_stg_asthma_ptnt_vsts --where patient_id=2240
--    WHERE PATIENT_ID=43637 and NETWORK='NBN'
--    WHERE PATIENT_ID=2810840 and NETWORK='SMN'

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
        JOIN ref_drug_descriptions rd
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
        JOIN ref_drug_descriptions rd
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
        JOIN ref_drug_descriptions rd
          ON rx.drug_description=rd.drug_description
         AND rd.drug_type_id=43  
      )
  ),
medication_mngmnt AS
  (
    SELECT
--      msrmnt_yr_end_dt,
      network,	
      patient_id,	
      order_dt,	
      rx_yr_end_dt,
      drug_name,	
      drug_description,	
      rx_quantity,
      frequency,	
      drug_frequency_num_val,
      route,
      dosage,	
      dosage_num_val,
      CASE 
        WHEN ROUTE='inhalation'
        THEN rx_quantity/(dosage_num_val*drug_frequency_num_val)  --dosage/num_val 
        ELSE rx_quantity/drug_frequency_num_val
      END days_covered_per_rx,     
      ROUND
      (
        CASE 
          WHEN EXTRACT(YEAR FROM order_dt) = EXTRACT(YEAR from rx_yr_end_dt)
          THEN 
            SUM
              (
                CASE 
                  WHEN ROUTE='inhalation'
                  THEN rx_quantity/(dosage_num_val*drug_frequency_num_val) --dosage/num_val 
                  ELSE rx_quantity/drug_frequency_num_val
                END
              )
            OVER (PARTITION BY network,patient_id,EXTRACT(YEAR FROM order_dt) ORDER BY NULL)
          END
      ) AS sum_days_covered_in_yr,      
      (rx_yr_end_dt - MIN(order_dt) OVER (PARTITION BY network,patient_id,extract(year from order_dt) ORDER BY order_dt)) treatment_days_in_year,
      MIN(order_dt) OVER (PARTITION BY network,patient_id ORDER BY order_dt) asthma_earlst_rx_dt,
      ROW_NUMBER() OVER (PARTITION BY network,patient_id ORDER BY order_dt) asthma_earlst_rx_rnum
    FROM
    (
      SELECT --+ parallel(8) 
        DISTINCT 
--        a.msrmnt_yr_end_dt,
        rx.network,	
        rx.patient_id,	
        rx.order_dt,	
        ADD_MONTHS(TRUNC (order_dt,'YEAR'),12)-1 rx_yr_end_dt,
        rx.drug_name,	
        rx.drug_description,	
        rx.rx_quantity,
        rx.dosage,	
        rx.frequency,	
        nvl(t2.drug_frequency_num_val,1) drug_frequency_num_val,
        nvl(t3.drug_frequency_num_val,1) dosage_num_val,
        rx.rx_exp_dt,	
        rd.route
      FROM fact_prescriptions rx 
      JOIN tr13_ref_drug_descriptions rd
        ON rx.drug_description=rd.drug_description
      AND rd.drug_type_id =44 --Number of people who achieved a proportion of days covered for their asthma controller medications, hence only criterion_id 44
      JOIN asthma_ptnt_lkp a
        ON a.patient_id=rx.patient_id
      AND a.network=rx.network
      AND a.ltst_ptnt_rec=1
      AND rx.order_dt BETWEEN a.begin_dt AND a.end_dt
--      JOIN dt
--      ON  rx.order_dt BETWEEN dt.begin_dt AND dt.end_dt
      LEFT JOIN ganesh_ref_drug_frequency t2 
        ON LOWER(rx.frequency) LIKE t2.drug_frequency 
       AND t2.med_route='oral'
      LEFT JOIN ganesh_ref_drug_frequency t3 
        ON lower(rx.dosage) like t3.drug_frequency
       AND t3.med_route='inhalation' 
--      where RX.PATIENT_ID=43637 AND RX.NETWORK='NBN'
--    WHERE RX.PATIENT_ID=2810840 and RX.NETWORK='SMN'
--     where rx.patient_id=2204 and rx.network='CBN' --and rx.drug_name='fluticasone propionate - nasal spray' --and trunc(order_dt)=date '2016-07-19'
--      where rx.patient_id=5087 and network='CBN'
    )   
  )
SELECT -- parallel 
--  SUBSTR(ORA_DATABASE_NAME,1,3) as network,
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
  ptnt.emp_provider_id pcp_id,
  ptnt.emp_provider_name pcp_name,
  f.visit_id last_pcp_visit_id,
  f.admission_date_time last_pcp_visit_dt,
  a.latest_visit_type,
  a.initial_visit_type,
  a.admission_date_time,
  a.discharge_date_time,
  pm.payer_name,
  pm.payer_group,
  a.fin_plan_name plan_name,
  CASE 
    WHEN UPPER(pm.payer_group) LIKE '%MEDICAID%' THEN 'Y' 
    ELSE 'N' 
  END AS medicaid_ind,  
  a.icd_code,	
  a.problem_description,
  g.drug_name asthma_erlst_med_name,
  g.route,
  g.dosage,
  g.frequency,
  g.asthma_earlst_rx_dt,
  g.sum_days_covered_in_yr,
  g.treatment_days_in_year,
  TO_CHAR
  (
    CASE 
      WHEN g.treatment_days_in_year=0 
      THEN 0
      WHEN g.treatment_days_in_year < g.sum_days_covered_in_yr
      THEN 1.00
      WHEN g.treatment_days_in_year >  g.sum_days_covered_in_yr
      THEN ROUND((g.sum_days_covered_in_yr/g.treatment_days_in_year),2)
    END,
    9.99
  ) proportion_days_covered,
  CASE
    WHEN 
    (  
      CASE 
        WHEN g.treatment_days_in_year=0 
        THEN 0
        WHEN g.treatment_days_in_year < g.sum_days_covered_in_yr
        THEN 1.00
        WHEN g.treatment_days_in_year >  g.sum_days_covered_in_yr
        THEN ROUND((g.sum_days_covered_in_yr/g.treatment_days_in_year),2)
      END
    ) >= 0.75
    THEN 'Y'
    ELSE 'N'
  END "numeraor_flag_75%_med_ratio",
  CASE
    WHEN 
    (  
      case 
        when  g.treatment_days_in_year=0 
        then 0
        when g.treatment_days_in_year < g.sum_days_covered_in_yr
        then 1.00
        when g.treatment_days_in_year >  g.sum_days_covered_in_yr
        then round((g.sum_days_covered_in_yr/g.treatment_days_in_year),2)
      end
    ) >= 0.50
    THEN 'Y'
    ELSE 'N'
  END "numeraor_flag_50%_med_ratio"
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
LEFT JOIN medication_mngmnt g
  ON g.patient_id=a.patient_id
 AND g.network=a.network 
 AND g.asthma_earlst_rx_rnum=1
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