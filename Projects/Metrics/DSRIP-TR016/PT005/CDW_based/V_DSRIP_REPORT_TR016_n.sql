CREATE OR REPLACE VIEW v_dsrip_report_tr016_n AS
WITH
  -- 07-Mar-2018, OK: created
  report_dates AS
  (
    SELECT --+ materialize
      NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
      ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')), -12) year_back_dt
    FROM dual
  ),
  prescriptions AS
  (
    SELECT --+ materialize
      pr.network,
      pr.facility_id,
      pr.patient_id,
      NVL(TO_CHAR(mdm.eid), pr.network||'-'||pr.patient_id) AS patient_gid, 
      NVL(dnm.drug_type_id, dscr.drug_type_id) AS drug_type_id,
      NVL(dnm.drug_name, dscr.drug_description) medication,
      pr.order_dt AS start_dt,
      NVL(pr.rx_dc_dt, DATE '9999-12-31') AS stop_dt,
      ROW_NUMBER() OVER
      (
        PARTITION BY NVL(TO_CHAR(mdm.eid), pr.network||'-'||pr.patient_id), NVL(dnm.drug_type_id, dscr.drug_type_id)
        ORDER BY pr.order_dt DESC
      ) rnum
    FROM report_dates dt
    JOIN fact_prescriptions pr
      ON pr.order_dt <= dt.year_back_dt AND pr.network NOT IN ('QHN','SBN') -- exclude Networks that switched to EPIC
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
      ON mdm.network = pr.network AND mdm.patientid = TO_CHAR(pr.patient_id) AND mdm.epic_flag = 'N'
    LEFT JOIN ref_drug_names dnm
      ON dnm.drug_name = pr.drug_name 
    LEFT JOIN ref_drug_descriptions dscr
      ON dscr.drug_description = pr.drug_description 
    WHERE dnm.drug_type_id IN (33, 34) OR dscr.drug_type_id IN (33, 34) -- Diabetes and Antipsychotic Medications
  ),
  bh_prescriptions AS
  (
    SELECT --+ materialize
      patient_gid, network, facility_id, patient_id, medication,
      ROW_NUMBER() OVER(PARTITION BY patient_gid ORDER BY start_dt DESC) rnum
    FROM prescriptions
    WHERE drug_type_id = 34 -- Antipsychotic Prescriptions
  ),
  diabetes_prescriptions AS
  (
    SELECT --+ materialize
      patient_gid, MIN(start_dt) start_dt, MAX(stop_dt) stop_dt
    FROM prescriptions
    WHERE drug_type_id = 33 -- Diabetes Prescriptions
    GROUP BY patient_gid
  ),
  patient_info AS
  ( 
    SELECT --+ materialize
      pat.network, pat.patient_id,
      NVL(TO_CHAR(mdm.eid), pat.network||'-'||pat.patient_id) patient_gid,
      pat.name AS patient_name,
      pat.medical_record_number,
      pat.birthdate,
      TRUNC(MONTHS_BETWEEN(dt.report_dt, pd.birthdate)/12) age,
      pat.street_address,
      pat.apt_suite,
      pat.city,
      pat.state,
      pat.mailing_code zip_code,
      pat.pcp_provider_name AS prim_care_provider, 
      NVL2(dep.service_type, f.facility_name, NULL) AS pcp_visit_facility, 
      NVL2(dep.service_type, vst.visit_number, NULL) AS pcp_visit_number, 
      NVL2(dep.service_type, TRUNC(vst.admission_date_time), NULL) AS pcp_visit_dt,
      ROW_NUMBER() OVER
      (
        PARTITION BY NVL(TO_CHAR(mdm.eid), pat.network||'-'||pat.patient_id)
        ORDER BY NVL2(dep.service_type, TRUNC(vst.admission_date_time), NULL) DESC NULLS LAST
      ) rnum
    FROM
    (
      SELECT DISTINCT
        network, patient_id
      FROM bh_prescriptions 
    ) bhp
    CROSS JOIN report_dates dt
    JOIN cdw.dim_patients pat
      ON pat.network = bhp.network AND pat.patient_id = bhp.patient_id AND pat.current_flag = 1
     AND pat.birthdate > ADD_MONTHS(dt.report_dt, -12*65) -- not 65 yet
     AND pat.birthdate <= ADD_MONTHS(dt.report_dt, -12*18) -- 18 or older
     AND pd.date_of_death IS NULL    
    JOIN cdw.visit vst
      ON vst.network = pr.network AND vst.patient_id = pat.patient_id
    JOIN cdw.dim_hc_facilities f
      ON f.network = vst.network AND f.facility_id = vst.facility_id
    LEFT JOIN cdw.visit_segment_visit_location vl
      ON vl.network = vst.network AND vl.visit_id = vst.visit_id
    LEFT JOIN cdw.dim_hc_departments dep
      ON dep.network = vl.network AND dep.location_id = vl.location_id AND dep.service_type = 'PCP'
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
      ON mdm.network = p.network AND TO_NUMBER(mdm.patientid) = p.patient_id AND mdm.epic_flag = 'N'
  ),
  diagnoses AS
  (
    SELECT --+ materialize
      pat.patient_gid,
      lkp.criterion_id diag_type_id, 
      DECODE(pd.diag_coding_scheme, '5', 'ICD-9', 'ICD-10') coding_scheme,
      pd.diag_code, pd.diag_description, pd.onset_date onset_dt, pd.stop_date stop_dt
    FROM patient_info pat
    JOIN patient_diag_dimension pd
      ON pd.network = bhp.network AND pd.patient_id = bhp.patient_id
     AND pd.network NOT IN ('QHN','SBN')
     AND pd.diag_coding_scheme IN (5, 10) AND pd.current_flag = '1' AND pd.stop_date IS NULL
    JOIN meta_conditions lkp
      ON lkp.qualifier = DECODE(pd.diag_coding_scheme, '5', 'ICD9', 'ICD10')
     AND lkp.value = pd.diag_code AND lkp.criterion_id IN (6, 31, 32) -- 6-DIABETES, 31-SCHIZOPHRENIA, 32-BIPOLAR
  ),
  diabetes_diagnoses AS
  (
    SELECT --+ materialize
      patient_gid, MIN(onset_dt) onset_dt, MAX(stop_dt) stop_dt
    FROM diagnoses
    WHERE diag_type_id = 6
    GROUP BY patient_gid
  ),
  psychotic_diagnoses AS
  (
    SELECT --+ materialize
      patient_gid, coding_scheme, diag_code, diag_description,
      ROW_NUMBER() OVER(PARTITION BY patient_gid ORDER BY onset_dt DESC, coding_scheme DESC, diag_code) rnum  
    FROM diagnoses
    WHERE diag_type_id IN (31, 32)
  ),
  a1c_glucose_tests AS
  (
    SELECT --+ ordered full(r) use_hash(e) use_hash(v) use_hash(mdm) materialize
      pat.patient_gid,
      pat.network,
      pat.patient_id,
      vst.facility_id,
      vst.visit_id,
      vst.visit_number,
      vst.visit_type_id,
      vt.name visit_type,
      vst.admission_date_time admission_dt,
      vst.discharge_date_time discharge_dt,
      mc.criterion_id test_type_id,
      e.event_id,
      e.date_time AS result_dt,
      r.data_element_id,
      rf.name data_element_name,
      r.value result_value,
      ROW_NUMBER() OVER(PARTITION BY pat.patient_gid ORDER BY e.date_time DESC, r.data_element_id) rnum
    FROM report_dates dt
    CROSS JOIN patient_info pat
    JOIN cdw.visit vst
      ON vst.network = pat.network AND vst.patient_id = pat.patient_id
     AND vst.admission_date_time >= dt.year_back_dt
     AND vst.admission_date_time < dt.report_dt   
    JOIN cdw.event e
      ON e.network = r.network AND e.visit_id = vst.visit_id
     AND e.date_time >= dt.year_back_dt
     AND e.date_time < db.report_dt 
    JOIN meta_conditions mc
      ON mc.network = e.network AND mc.criterion_id IN (4, 23) -- A1C and Glucose Level results
    JOIN cdw.result r
      ON r.network = e.network AND r.visit_id = e.visit_id AND r.event_id = e.event_id
     AND r.data_element_id = mc.value
    JOIN cdw.result_field rf
      ON rf.network = r.network AND rf.data_element_id = r.data_element_id
    JOIN cdw.ref_visit_types vt
      ON vt.visit_type_id = v.visit_type_id
  )
SELECT --+ USE_HASH(f pd pm pr)
  dt.report_dt AS report_period_start_dt,
  pat.patient_gid,
  NVL(tst.network, pat.network) network,
  NVL(tst.facility_id, amed.facility_id) facility_id,
  NVL(f.facility_name, 'Unknown') facility_name,
  NVL(tst.patient_id, amed.patient_id) patient_id, 
  pat.name AS patient_name,
  pat.medical_record_number,
  pat.birthdate,
  pat.age,
  pat.street_address,
  pd.apt_suite,
  pd.city,
  pd.state,
  pd.mailing_code zip_code,
  amed.medication,
  CASE WHEN psych.coding_scheme IS NOT NULL THEN psych.coding_scheme||': '||psych.diag_code END bh_diag_code,
  psych.diag_description bh_diagnosis,
  pcp.prim_care_provider,
  pcp.pcp_visit_dt AS last_pcp_visit_dt,
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
  ROW_NUMBER() OVER
  (
    PARTITION BY amed.patient_gid, tst.network, tst.visit_id
    ORDER BY CASE WHEN pm.payer_group = 'Medicaid' THEN 1 ELSE 2 END, pr.payer_rank
  ) rnum  
FROM patient_info pat
JOIN bh_prescriptions amed
  ON amed.patient_gid = pat.patient_gid AND amed.rnum = 1
LEFT JOIN diabetes_diagnoses diab
  ON diab.patient_gid = amed.patient_gid 
LEFT JOIN psychotic_diagnoses psych
  ON psych.patient_gid = amed.patient_gid AND psych.rnum = 1
LEFT JOIN diabetes_prescriptions dmed
  ON dmed.patient_gid = amed.patient_gid
LEFT JOIN a1c_glucose_tests tst
  ON tst.patient_gid = amed.patient_gid AND tst.rnum = 1
LEFT JOIN cdw.dim_hc_facilities f
  ON f.network = NVL(tst.network, amed.network) AND f.facility_id = amed.facility_id
LEFT JOIN cdw.visit_segment_payer pr
  ON pr.network = tst.network AND pr.visit_id = tst.visit_id
LEFT JOIN pt008.payer_mapping pm
  ON pm.network = pr.network AND pm.payer_id = pr.payer_id
WHERE pat.rnum = 1
AND
(
  (diab.onset_dt IS NULL OR diab.onset_dt > dt.year_back_dt) -- no Diabetes prior to last year
  AND (dmed.start_dt IS NULL OR dmed.start_dt > dt.year_back_dt) -- no Diabetes Medications taken prior to last year
);
