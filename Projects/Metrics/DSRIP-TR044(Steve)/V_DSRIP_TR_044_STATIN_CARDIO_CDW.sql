CREATE OR REPLACE FORCE VIEW  V_DSRIP_TR044_STAT_CARDIO_CDW

 AS
 WITH report_dates 
-- ************15-JUN-2018, GK: INITIAL VERSION

--- *******DENOMINATOR NCLUSIONS 
        --  MALES (21-75 YEARS OLD) AND FEMALES (40-75 YEARS OLD)  ************
    --71	DIAGNOSES:ISCHEMIC VASCULAR DISEASE (IVD)	List The list of Ischemic Vascular (IVD) diagnoses
    --70	DIAGNOSES:MYOCARDIAL INFARCTION (MI)	List The list of Myocardial Infarction (MI) diagnoses

-- *******DENOMINATOR  EXCLUSONS ************************************************************************
--73	DIAGNOSES:PREGNANCY ICD CODES	The List Pregnancy Icd Codes Diagnoses
--85	DIAGNOSES:MYALGIA, MYOSITIS, MYOPATHY
--84	DIAGNOSES:CIRRHOSIS ICD CODES
--83	DIAGNOSES:END-STAGE RENAL DISEASES
--86	MEDICATIONS:CLOMIFENE TREATS INFERTILITY IN WOMEN
--************* NUMERATOR	******************************************************************************
--72	MEDICATIONS:STATIN MEDICATIONS	The list of Statin Medications
  AS
  (
      SELECT --+ materialize
      TRUNC(SYSDATE, 'MONTH') report_dt,
      ADD_MONTHS(TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH'), -24)   start_dt,
      ADD_MONTHS(TRUNC((TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH') - 1),  'YEAR'), 12) - 1      AS msrmnt_year_end_dt
      FROM  DUAL
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
    ROW_NUMBER() OVER(PARTITION BY pd.network, pd.patient_id, mc.criterion_id ORDER BY pd.onset_date DESC) AS cnt
    FROM  report_dates d
    CROSS JOIN meta_conditions mc
    JOIN cdw.fact_patient_diagnoses pd ON pd.diag_code = mc.VALUE
    JOIN cdw.dim_patients pp ON pp.network = pd.network AND pp.patient_id = pd.patient_id AND current_flag = 1 AND  sex is not null
    WHERE
    mc.criterion_id IN (70, 71) --AND Pd.status_id IN (0, 6, 7,8)
    AND FLOOR((d.msrmnt_year_end_dt - pp.birthdate)/365) 
          BETWEEN (CASE  WHEN LOWER(sex) = 'male' THEN 21 ELSE 40 END) AND 75 AND pd.onset_date >= start_dt
   ),
  pat_combo AS
  (
    SELECT
    report_dt,
    start_dt,
    network,
    sex,
    patient_id,
    mi_diag_code,
    mi_diagnosis_name,
    mi_onset_dt,
    ivd_diag_code,
    ivd_diagnosis_name,
    ivd_onset_dt
    FROM
       ( 
         SELECT *  FROM pat_all_diag WHERE cnt = 1 AND crit_id IN (70, 71)
        )
             PIVOT
              (MAX(diag_code) AS diag_code, MAX(problem_comments) AS diagnosis_name, MAX(diag_dt) AS onset_dt
              FOR crit_id
              IN (70 AS mi, 71 AS ivd))
   ),
   excl_ptnts AS
   (
      SELECT --+ materialize
      DISTINCT pd.network, pd.patient_id
      FROM  cdw.fact_patient_diagnoses pd
      JOIN meta_conditions mc ON mc.VALUE = pd.diag_code AND mc.criterion_id IN (73,  85,84,  83)
    UNION ALL
      SELECT
      DISTINCT d.network, d.patient_id
      FROM   report_dates d
      CROSS JOIN cdw.fact_patient_prescriptions d 
      JOIN cdw.ref_drug_descriptions rd  ON rd.drug_description = d.drug_description AND rd.drug_type_id = 86
      WHERE  order_dt >= start_dt
    ),
      denominator AS
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
		  ROW_NUMBER() OVER(PARTITION BY vst.network, vst.patient_id ORDER BY vst.admission_dt DESC) vst_rnum
		  FROM
		  pat_combo pc
		  JOIN cdw.fact_visits vst
		  ON vst.network = pc.network AND vst.patient_id = pc.patient_id AND vst.admission_dt >= pc.start_dt
		  LEFT JOIN cdw.fact_visit_payers fct_pyrs ON fct_pyrs.visit_key = vst.visit_key
		  LEFT JOIN cdw.dim_payers pyr1 ON pyr1.payer_key = fct_pyrs.first_payer_key
		  LEFT JOIN cdw.dim_payers pyr2 ON pyr2.payer_key = fct_pyrs.first_payer_key
		  LEFT JOIN cdw.dim_payers pyr3 ON pyr3.payer_key = fct_pyrs.first_payer_key
		  LEFT JOIN cdw.ref_financial_class fc
		  ON fc.network = vst.network AND fc.financial_class_id = vst.financial_class_id
		  LEFT JOIN cdw.dim_hc_facilities fclty ON fclty.facility_key = vst.facility_key
		WHERE
		NOT EXISTS
		(
		 SELECT  1
		 FROM excl_ptnts ep
		 WHERE ep.network = pc.network AND ep.patient_id = pc.patient_id
		)
    ),
  statin_rxs 
    AS
 (
   SELECT --+ materialize
   fpd.network,
   fpd.patient_id,
   fpd.order_dt,
   fpd.drug_description,
   fpd.rx_quantity,
   ROW_NUMBER() OVER(PARTITION BY fpd.network, fpd.patient_id ORDER BY fpd.order_dt DESC) AS rx_rnum
  FROM
   report_dates dt
   JOIN cdw.fact_patient_prescriptions fpd ON fpd.order_dt >= dt.start_dt
   JOIN denominator d ON d.network = fpd.network AND d.patient_id = fpd.patient_id
   JOIN cdw.ref_drug_descriptions rd
    ON rd.drug_description = fpd.drug_description AND rd.drug_type_id = 72 
 ),
  cardio_clinic_vsts AS
 (
    SELECT --+ materialize
    v.network,
    v.patient_id,
    v.visit_id cardio_visit_id,
    v.admission_dt cardio_visit_dt,
    fd.facility_name AS cardio_vst_facility_name,
    v.attending_provider_key cardio_vst_provider_key,
    ROW_NUMBER() OVER(PARTITION BY v.network, v.patient_id ORDER BY v.admission_dt DESC) cardio_rnum
    FROM
    denominator diag
    JOIN cdw.fact_visits v
    ON v.network = diag.network AND v.patient_id = diag.patient_id AND v.admission_dt >= start_dt
    JOIN cdw.dim_hc_departments d
    ON d.department_key = v.last_department_key AND d.service_type = 'CARDIO'
    JOIN cdw.dim_hc_facilities fd ON fd.facility_key = v.facility_key
  ),
 pcp_clinic_vsts AS
  (
    SELECT --+ materialize
    v.network,
    v.patient_id,
    v.visit_id pcp_visit_id,
    v.admission_dt pcp_visit_dt,
    fd.facility_name AS pcp_vst_facility_name,
    v.attending_provider_key pcp_vst_provider_key,
    ROW_NUMBER() OVER(PARTITION BY v.network, v.patient_id ORDER BY v.admission_dt DESC) pcp_rnum
    FROM
    denominator diag
    JOIN cdw.fact_visits v
    ON v.network = diag.network AND v.patient_id = diag.patient_id AND v.admission_dt >= start_dt
    JOIN cdw.dim_hc_departments d ON d.department_key = v.last_department_key AND d.service_type = 'PCP'
    JOIN cdw.dim_hc_facilities fd ON fd.facility_key = v.facility_key
  )
 SELECT
  dnmr.report_dt,
  dnmr.network,
  NVL(dnmr.facility_name, 'Unknown') facility_name,
  dnmr.patient_id,
  p.name,
  p.birthdate,
  NVL(sc.second_mrn, p.medical_record_number) AS mrn,
  dnmr.visit_id,
  dnmr.visit_number,
  p.home_phone,
  p.day_phone,
  dnmr.financial_class_name,
  dnmr.first_payer,
  dnmr.second_payer,
  dnmr.third_payer,
  p.pcp_provider_name AS assigned_pcp,
  pcp.pcp_visit_dt,
  pcp.pcp_visit_id,
  prvdr1.provider_name pcp_vst_provider,
  pcp_vst_facility_name,
  cardio.cardio_visit_dt,
  cardio.cardio_visit_id,
  prvdr2.provider_name cardio_vst_provider_name,
  cardio_vst_facility_name,
  dnmr.mi_diagnosis_name,
  dnmr.mi_onset_dt,
  dnmr.ivd_diagnosis_name,
  dnmr.ivd_onset_dt,
  rx.order_dt AS statin_rx_dt,
  rx.drug_description AS statin_rx_name,
  rx.rx_quantity AS statin_rx_quantity,
  DECODE(rx.patient_id, NULL, 'N', 'Y') AS numerator_flag,
  DECODE(pcp.pcp_visit_id, NULL, 0, 1) AS pcp_flag,
  DECODE(cardio.cardio_visit_id, NULL, 0, 1) AS cardio_flag,
  CASE WHEN pcp.pcp_visit_id IS NULL AND cardio.cardio_visit_id IS NULL THEN 1 ELSE 0 END AS non_pcp_flag
 FROM
  denominator dnmr
  JOIN cdw.dim_patients p  ON p.network = dnmr.network AND p.patient_id = dnmr.patient_id AND p.current_flag = 1
  LEFT JOIN pcp_clinic_vsts pcp  ON pcp.network = dnmr.network AND pcp.patient_id = dnmr.patient_id AND pcp.pcp_rnum = 1
  LEFT JOIN cardio_clinic_vsts cardio ON cardio.network = dnmr.network AND cardio.patient_id = dnmr.patient_id AND cardio.cardio_rnum = 1
  LEFT JOIN statin_rxs rx ON rx.network = dnmr.network AND rx.patient_id = dnmr.patient_id AND rx.rx_rnum = 1
  LEFT JOIN cdw.ref_patient_secondary_mrn sc   ON sc.network = dnmr.network AND sc.facility_key = dnmr.facility_key AND sc.patient_id = dnmr.patient_id
  LEFT JOIN cdw.dim_providers prvdr1 ON prvdr1.provider_key = pcp_vst_provider_key 
  LEFT JOIN cdw.dim_providers prvdr2 ON prvdr2.provider_key = cardio_vst_provider_key
 WHERE
  dnmr.vst_rnum = 1;
