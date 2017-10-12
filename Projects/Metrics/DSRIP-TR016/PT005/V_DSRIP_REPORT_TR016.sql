CREATE OR REPLACE VIEW v_dsrip_report_tr016 AS
WITH
  report_dates AS
  (
    SELECT --+ materialize
      NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
      ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')), -12) year_back_dt
    FROM dual
  ),
  prescriptions AS
  (
    SELECT --+ materialize parallel(8)
      pr.network,
      pr.facility_id,
      pr.patient_id,
      pr.mrn,
      NVL(TO_CHAR(mdm.eid), pr.network||'-'||pr.patient_id) AS patient_gid, 
      NVL(dnm.drug_type_id, dscr.drug_type_id) AS drug_type_id,
      NVL(dnm.drug_name, dscr.drug_description) medication,
      pr.order_dt AS start_dt,
      NVL(pr.rx_dc_dt, DATE '9999-12-31') AS stop_dt,
      ROW_NUMBER() OVER(PARTITION BY NVL(TO_CHAR(mdm.eid), pr.network||'-'||pr.patient_id), NVL(dnm.drug_type_id, dscr.drug_type_id) ORDER BY pr.order_dt DESC) rnum
    FROM report_dates rd
    JOIN fact_prescriptions pr ON pr.order_dt <= rd.year_back_dt
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
     ON mdm.network = pr.network AND TO_NUMBER(mdm.patientid) = pr.patient_id AND mdm.epic_flag = 'N'
    LEFT JOIN ref_drug_names dnm ON dnm.drug_name = pr.drug_name 
    LEFT JOIN ref_drug_descriptions dscr ON dscr.drug_description = pr.drug_description 
    WHERE dnm.drug_type_id IN (33, 34) OR dscr.drug_type_id IN (33, 34) -- Diabetes and Antipsychotic Medications
  ),
  diabetes_diagnoses AS
  (
    SELECT --+ materialize parallel(8)
      NVL(TO_CHAR(mdm.eid), pd.network||'-'||pd.patient_id) patient_gid,
      MIN(pd.onset_date) onset_dt,
      MAX(pd.stop_date) stop_dt
    FROM patient_diag_dimension pd
    JOIN meta_conditions lkp
      ON lkp.qualifier = DECODE(pd.diag_coding_scheme, '5', 'ICD9', 'ICD10')
     AND lkp.value = pd.diag_code AND lkp.criterion_id = 6 -- DIAGNOSIS:DIABETES
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
      ON mdm.network = pd.network AND TO_NUMBER(mdm.patientid) = pd.patient_id AND mdm.epic_flag = 'N'
    WHERE pd.diag_coding_scheme IN (5, 10) AND pd.current_flag = '1' AND pd.stop_date IS NULL
    GROUP BY NVL(TO_CHAR(mdm.eid), pd.network||'-'||pd.patient_id)
  ),
  diabetes_prescriptions AS
  (
    SELECT --+ materialize
      patient_gid, MIN(start_dt) start_dt, MAX(stop_dt) stop_dt
    FROM prescriptions
    WHERE drug_type_id = 33 -- Diabetes Prescriptions
    GROUP BY patient_gid
  ),
  a1c_glucose_tests AS
  (
    SELECT --+ materialize
      NVL(TO_CHAR(mdm.eid), a1c.network||'-'||a1c.patient_id) patient_gid,
      a1c.network,
      a1c.facility_id,
      a1c.patient_id,
      a1c.test_type_id,
      a1c.visit_id,
      a1c.visit_number,
      a1c.visit_type_id,
      a1c.visit_type,
      a1c.admission_dt,
      a1c.discharge_dt,
      a1c.result_dt,
      a1c.data_element_name,
      a1c.result_value,
      ROW_NUMBER() OVER(PARTITION BY NVL(TO_CHAR(mdm.eid), a1c.network||'-'||a1c.patient_id) ORDER BY a1c.result_dt DESC) rnum
    FROM report_dates dt
    JOIN dsrip_tr016_a1c_glucose_rslt a1c
      ON a1c.result_dt >= dt.year_back_dt AND a1c.result_dt < dt.report_dt 
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
      ON mdm.network = a1c.network AND TO_NUMBER(mdm.patientid) = a1c.patient_id AND mdm.epic_flag = 'N'
  )
SELECT
  dt.report_dt AS report_period_start_dt,
  amed.patient_gid,
  NVL(tst.network, amed.network) network,
  NVL(tst.facility_id, amed.facility_id) facility_id,
  NVL(f.facility_name, 'Unknown') facility_name,
  NVL(tst.patient_id, amed.patient_id) patient_id, 
  pd.name AS patient_name,
  pd.medical_record_number,
  pd.birthdate,
  TRUNC(MONTHS_BETWEEN(NVL(pd.date_of_death, SYSDATE), pd.birthdate)/12) age,
  amed.medication,
  tst.visit_id,
  tst.visit_number,
  tst.visit_type_id,
  tst.visit_type,
  tst.admission_dt,
  tst.discharge_dt,
  pm.payer_group,
  pr.payer_id,
  pm.payer_name,
  tst.test_type_id,
  tst.result_dt,
  tst.data_element_name,
  tst.result_value,
  ROW_NUMBER() OVER(PARTITION BY amed.patient_gid, tst.network, tst.visit_id ORDER BY CASE WHEN pm.payer_group = 'Medicaid' THEN 1 ELSE 2 END, pr.payer_rank) rnum  
  --pcp, medicaid_ind, plan_id, plan_name, icd_code
FROM prescriptions amed
CROSS JOIN report_dates dt
LEFT JOIN diabetes_diagnoses diab ON diab.patient_gid = amed.patient_gid
LEFT JOIN diabetes_prescriptions dmed ON dmed.patient_gid = amed.patient_gid
LEFT JOIN a1c_glucose_tests tst ON tst.patient_gid = amed.patient_gid AND tst.rnum = 1
LEFT JOIN patient_dimension pd
  ON pd.network = NVL(tst.network, amed.network) AND pd.patient_id = NVL(tst.patient_id, amed.patient_id) AND pd.current_flag = 1 
LEFT JOIN facility_dimension f ON f.network = NVL(tst.network, amed.network) AND f.facility_id = amed.facility_id
LEFT JOIN dsrip_tr016_payers pr ON pr.network = tst.network AND pr.visit_id = tst.visit_id
LEFT JOIN pt008.payer_mapping pm ON pm.network = pr.network AND pm.payer_id = pr.payer_id
WHERE amed.drug_type_id = 34 -- Antipsychotic Medications
AND amed.rnum = 1
AND
(
  (diab.onset_dt IS NULL OR diab.onset_dt > dt.year_back_dt) -- no Diabetes prior last year
  AND (dmed.start_dt IS NULL OR dmed.start_dt > dt.year_back_dt) -- no Diabetes Medications taken prior last year
);