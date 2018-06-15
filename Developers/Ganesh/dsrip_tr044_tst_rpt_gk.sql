CREATE TABLE dsrip_tr044_tst_rpt_gk
NOLOGGING
PARALLEL 32
AS
WITH report_dates 
AS
  (
    SELECT --+ materialize
      TRUNC(SYSDATE, 'MONTH') report_dt, 
      ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) start_dt,
      ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS msrmnt_year_end_dt
    FROM dual
  ),
  pat_all_diag AS
  (
    SELECT --+  materialize 
      d.report_dt, 
      d.start_dt, 
      pp.sex, 
      pd.network, 
      pd.patient_id, 
      pd.onset_date AS diag_dt, 
      mc.criterion_id AS crit_id, 
      pd.diag_code, 
      pd.problem_comments,
      ROW_NUMBER() OVER (PARTITION BY pd.network, pd.patient_id, mc.criterion_id ORDER BY pd.onset_date DESC) cnt
    FROM report_dates d 
    CROSS JOIN  meta_conditions mc
    JOIN fact_patient_diagnoses pd ON pd.diag_code = mc.VALUE 
    --AND pd.patient_id = 77465 AND pd.network = 'CBN'
--    AND pd.patient_id = 318397 AND pd.network = 'GP1'
    JOIN dim_patients pp ON pp.network  = pd.network AND pp.patient_id  = pd.patient_id AND current_flag  = 1 AND LOWER(sex) IN ('male', 'female')  
    WHERE  mc.criterion_id IN (70,71)  --AND Pd.status_id IN (0, 6, 7,8)
    AND FLOOR((d.msrmnt_year_end_dt - pp.birthdate) / 365) BETWEEN (CASE WHEN LOWER(sex) = 'male' THEN  21 ELSE 40 END) AND 75
    and pd.onset_date >= start_dt 
  ),
  
--  select --+ parallel(32)
----  distinct network, patient_id 
--  *
--  from pat_all_diag;
  
  pat_combo
  AS 
  (
    SELECT  
      report_dt, start_dt, network, sex, patient_id, mi_diag_code, mi_diagnosis_name, mi_onset_dt, ivd_diag_code, ivd_diagnosis_name, ivd_onset_dt
    FROM
    (
      SELECT 
        * 
      FROM pat_all_diag
      WHERE cnt = 1 AND crit_id IN (70,71)
    )  
    PIVOT
    (
      MAX(diag_code) AS diag_code,
      MAX(problem_comments) AS diagnosis_name,
      MAX(diag_dt) AS onset_dt
      FOR crit_id in(70 AS mi, 71 AS ivd)
    )
  ),
--  select --+ parallel(32)
--  * from pat_combo;
  
  excl_ptnts AS
  (
    SELECT   --+ materialize
     DISTINCT pd.network, pd.patient_id
    FROM fact_patient_diagnoses pd
    JOIN meta_conditions mc
      ON mc.value = pd.diag_code AND mc.criterion_id in(73,85,84,83)
    
    UNION ALL 
    
    SELECT 
      DISTINCT d.network, d.patient_id
    FROM  report_dates d
    CROSS JOIN fact_patient_prescriptions d
    JOIN ref_drug_descriptions rd
    ON rd.drug_description = d.drug_description AND rd.drug_type_id = 86
    WHERE order_dt >= start_dt
  ),
  denominator 
  AS
  (
    SELECT --+ materialize
      pc.report_dt,
      pc.start_dt,
      pc.network,
      pc.patient_id,
      pc.sex,
      pc.mi_onset_dt,
      pc.mi_diag_code,
      pc.mi_diagnosis_name,
      pc.ivd_onset_dt,
      pc.ivd_diag_code,
      pc.ivd_diagnosis_name,
      vst.visit_id,
      vst.visit_number,
      vst.financial_class_id,
      fc.financial_class_name,
      vst.facility_key,
      fclty.facility_name,
      vst.admission_dt,
      pyr1.payer_name AS first_payer,
      pyr2.payer_name AS second_payer,
      pyr3.payer_name AS third_payer,
      ROW_NUMBER() OVER (PARTITION BY vst.network, vst.patient_id ORDER BY vst.admission_dt DESC) vst_rnum
    FROM pat_combo pc
    JOIN fact_visits vst 
      ON vst.network = pc.network AND vst.patient_id = pc.patient_id AND vst.admission_dt >= pc.start_dt
    LEFT JOIN fact_visit_payers fct_pyrs
      ON fct_pyrs.visit_key = vst.visit_key
    LEFT JOIN dim_payers pyr1
      ON pyr1.payer_key = fct_pyrs.first_payer_key
    LEFT JOIN dim_payers pyr2
      ON pyr2.payer_key = fct_pyrs.first_payer_key 
    LEFT JOIN dim_payers pyr3
      ON pyr3.payer_key = fct_pyrs.first_payer_key
    LEFT JOIN ref_financial_class fc
      ON fc.network = vst.network AND fc.financial_class_id = vst.financial_class_id
      LEFT JOIN dim_hc_facilities fclty
                    ON fclty.facility_key = vst.facility_KEY
    WHERE NOT EXISTS
    (
      SELECT 
        1 
      FROM excl_ptnts ep
      WHERE ep.network = pc.network AND ep.patient_id = pc.patient_id
    )
  ),
--  select --+ parallel(32) 
--  * from denominator;
  
  
  statin_rxs AS
  (
    SELECT  --+ materialize
      fpd.network, fpd.patient_id, fpd.order_dt, fpd.drug_description, fpd.rx_quantity, 
      ROW_NUMBER() OVER (PARTITION BY fpd.network, fpd.patient_id ORDER BY fpd.order_dt DESC) AS rx_rnum
    FROM report_dates dt
    JOIN fact_patient_prescriptions fpd ON fpd.order_dt >= dt.start_dt
    JOIN denominator d ON d.network = fpd.network AND d.patient_id = fpd.patient_id
    JOIN ref_drug_descriptions rd
      ON rd.drug_description = fpd.drug_description AND rd.drug_type_id = 72
--    WHERE order_dt >= start_dt  
  ),
--    select --+ parallel(32) 
--  * from statin_rxs;
  
  
  
  cardio_clinic_vsts AS
  (
    SELECT --+ materialize
      v.network, v.patient_id, v.visit_id cardio_visit_id, v.admission_dt cardio_visit_dt, fd.facility_name AS cardio_vst_facility_name, v.attending_provider_key cardio_vst_provider_key,
      ROW_NUMBER() OVER(PARTITION BY v.network, v.patient_id ORDER BY v.admission_dt DESC) cardio_rnum 
    FROM denominator diag
    JOIN cdw.fact_visits v ON v.network = diag.network  AND v.patient_id = diag.patient_id AND v.admission_dt >= start_dt
    JOIN cdw.dim_hc_departments d ON d.department_key = v.last_department_key AND d.service_type = 'CARDIO'
    JOIN dim_hc_facilities fd
      ON fd.facility_key = v.facility_key 
  ),
  pcp_clinic_vsts AS
  (
    SELECT --+ materialize
      v.network, v.patient_id, v.visit_id pcp_visit_id, v.admission_dt pcp_visit_dt, fd.facility_name AS pcp_vst_facility_name, v.attending_provider_key pcp_vst_provider_key,
      ROW_NUMBER() OVER(PARTITION BY v.network, v.patient_id ORDER BY v.admission_dt DESC) pcp_rnum 
    FROM denominator diag
    JOIN cdw.fact_visits v ON v.network = diag.network  AND v.patient_id = diag.patient_id AND v.admission_dt >= start_dt
    JOIN cdw.dim_hc_departments d ON d.department_key = v.last_department_key AND d.service_type = 'PCP'
    JOIN dim_hc_facilities fd
      ON fd.facility_key = v.facility_key 
  ) 
SELECT  -- parallel(32)
  dnmr.report_dt,
  dnmr.network,
  dnmr.patient_id,
  p.name,
  p.birthdate,
  nvl(sc.second_mrn, p.medical_record_number) AS mrn,
  dnmr.visit_id,
  dnmr.visit_number,
  p.home_phone,            
  p.day_phone,
--  p.address,
  dnmr.financial_class_name,
  dnmr.first_payer,
  dnmr.second_payer,
  dnmr.third_payer,
  p.pcp_provider_name AS assigned_pcp,
  pcp.pcp_visit_dt,
  prvdr1.provider_name pcp_vst_provider,
  pcp_vst_facility_name,
  dnmr.mi_diagnosis_name, 
  dnmr.mi_onset_dt,
  dnmr.ivd_diagnosis_name,
  dnmr.ivd_onset_dt,
  rx.order_dt AS statin_rx_dt,
  rx.drug_description AS statin_rx_name,
  rx.rx_quantity,
  --Number of Refills ?? we don't have refill info
  --Name of provider who prescribed the statin ?? we don't have prescribed provider info
  --Facility of provider who prescribed the statin ?? we don't have prescribed provider info
  --Date of last visit with non-PCP provider
  --Name of provider of last visit with a non PCP-Provider
  --Specialty/type of provider of last visit with a non-PCP provider
  --Date of Last Cardiology Appointment  (see "Primary Care_Cardiology Clinics" tab)
  cardio.cardio_visit_dt,
  prvdr2.provider_name cardio_vst_provider_name,
  decode(rx.patient_id, NULL, 'N','Y') numerator_flag
FROM denominator dnmr
JOIN dim_patients p
ON p.network = dnmr.network AND p.patient_id = dnmr.patient_id AND p.current_flag = 1
LEFT JOIN pcp_clinic_vsts pcp
ON pcp.network = dnmr.network AND pcp.patient_id = dnmr.patient_id AND pcp.pcp_rnum = 1
LEFT JOIN cardio_clinic_vsts cardio
ON cardio.network = dnmr.network AND cardio.patient_id = dnmr.patient_id AND cardio.cardio_rnum = 1
LEFT JOIN statin_rxs rx
ON rx.network = dnmr.network AND rx.patient_id = dnmr.patient_id AND rx.rx_rnum = 1
LEFT JOIN ref_patient_secondary_mrn sc
ON sc.network = dnmr.network AND sc.facility_key = dnmr.facility_key  AND sc.patient_id = dnmr.patient_id
LEFT JOIN dim_providers prvdr1
ON prvdr1.provider_key = pcp_vst_provider_key
LEFT JOIN dim_providers prvdr2
ON prvdr2.provider_key = cardio_vst_provider_key
WHERE dnmr.vst_rnum=1


