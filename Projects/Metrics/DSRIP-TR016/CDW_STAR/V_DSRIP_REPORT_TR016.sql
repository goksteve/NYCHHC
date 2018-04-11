CREATE OR REPLACE VIEW v_tst_gk_dsrip_report_tr016
AS
WITH
  -- 10-Apr-2018, GK: Converting into CDW Star schema
  -- 07-Feb-2018, OK: included psychotic diagnoses
  -- 16-Jan-2018, OK: added USE_HASH hints into the main query
  -- 12-Dec-2017, OK: excluded QHN and SBN networks
  report_dates AS
  (
    SELECT --+ materialize
      NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
      ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV','CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')), -12) year_back_dt
    FROM dual
  ),
--  select * from report_dates;
  prescriptions AS
  (
    SELECT --+ materialize
      pr.network,
      pr.facility_key,
      pr.patient_id,
      pr.mrn,
      NVL(TO_CHAR(mdm.eid), pr.network||'-'||pr.patient_id) AS patient_gid, 
      NVL(dnm.drug_type_id, dscr.drug_type_id) AS drug_type_id,
      NVL(dnm.drug_name, dscr.drug_description) medication,
      pr.order_dt AS start_dt,
      NVL(pr.rx_dc_dt, DATE '9999-12-31') AS stop_dt,
      ROW_NUMBER() OVER(PARTITION BY NVL(TO_CHAR(mdm.eid), pr.network||'-'||pr.patient_id), NVL(dnm.drug_type_id, dscr.drug_type_id) ORDER BY pr.order_dt DESC) rnum
    FROM report_dates rd
    JOIN FACT_PATIENT_PRESCRIPTIONS pr
      ON pr.order_dt <= rd.year_back_dt AND pr.network NOT IN ('QHN','SBN') -- exclude Networks that have switched to EPIC
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
      ON mdm.network = pr.network AND mdm.patientid = TO_CHAR(pr.patient_id) AND mdm.epic_flag = 'N'
    LEFT JOIN ref_drug_names dnm ON dnm.drug_name = pr.drug_name 
    LEFT JOIN ref_drug_descriptions dscr ON dscr.drug_description = pr.drug_description 
    WHERE dnm.drug_type_id IN (33, 34) OR dscr.drug_type_id IN (33, 34) -- Diabetes and Antipsychotic Medications
  ),
--  select --+ parallel(32) 
--  * from prescriptions;
  diagnoses AS
  (
    SELECT --+ materialize
      NVL(TO_CHAR(mdm.eid), pd.network||'-'||pd.patient_id) patient_gid,
      lkp.criterion_id diag_type_id, 
      DECODE(pd.diag_coding_scheme, '5', 'ICD-9', 'ICD-10') coding_scheme,
      pd.diag_code, 
      pd.problem_comments diag_description, 
      pd.onset_date onset_dt, 
      pd.end_date stop_dt
    FROM fact_patient_diagnoses pd
    JOIN meta_conditions lkp
      ON lkp.qualifier = DECODE(pd.diag_coding_scheme, 'ICD-9', 'ICD9', 'ICD10')
     AND lkp.value = pd.diag_code AND lkp.criterion_id IN (6, 31, 32) -- 6-DIABETES, 31-SCHIZOPHRENIA, 32-BIPOLAR
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
      ON mdm.network = pd.network AND mdm.patientid = TO_CHAR(pd.patient_id) AND mdm.epic_flag = 'N'
    WHERE pd.network NOT IN ('QHN','SBN') AND pd.diag_coding_scheme IN ('ICD-10', 'ICD-9') --AND pd.current_flag = '1' 
    AND pd.end_date IS NULL
  ),
--  select  --+ parallel(32)
--  count(1) from diagnoses;
--  919,956
  a1c_glucose_rslt AS
  (
    SELECT -- parallel(32)
      network, facility_key, patient_id, visit_id, visit_number, visit_type_id, visit_type,
      admission_dt, discharge_dt, first_payer_key, test_type_id, event_id, result_dt, data_element_id, /*data_element_name,*/ result_value
    FROM
    (
      SELECT --+ ordered use_hash(r e v rf)
        v.network,
        v.facility_key,
        v.patient_id,
        v.visit_id,
        v.visit_number,
        v.final_visit_type_id visit_type_id,
        vt.name visit_type,
        v.admission_dt,
        v.discharge_dt,
        v.first_payer_key,
        mc.criterion_id test_type_id,
        r.event_id,
        r.result_dt,
        r.data_element_id,
--        rf.name data_element_name,
        r.result_value,
        ROW_NUMBER() OVER(PARTITION BY v.network, v.patient_id ORDER BY r.event_id DESC, r.data_element_id) rnum
        FROM report_dates dt
        JOIN fact_results r
          ON r.result_dt >= dt.year_back_dt AND r.result_dt < dt.report_dt AND r.network = SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK')
        JOIN meta_conditions mc
          ON mc.network = r.network AND mc.criterion_id IN (4, 23) -- A1C and Glucose Level results
        JOIN fact_visits v
          ON v.network = r.network AND v.visit_id = r.visit_id          
        LEFT JOIN ref_visit_types vt
          ON vt.visit_type_id = v.final_visit_type_id
    )
    WHERE rnum = 1
  ),
--  select --+ parallel(32)
--  * from a1c_glucose_rslt;
  diabetes_diagnoses AS
  (
    SELECT --+ materialize
      patient_gid, MIN(onset_dt) onset_dt, MAX(stop_dt) stop_dt
    FROM diagnoses
    WHERE diag_type_id = 6
    GROUP BY patient_gid
  ),
--  select --+ parallel(32) 
--  count(distinct patient_gid) from diabetes_diagnoses;  
  --239,988 (more)
  psychotic_diagnoses AS
  (
    SELECT --+ materialize
      patient_gid, coding_scheme, diag_code, diag_description,
      ROW_NUMBER() OVER(PARTITION BY patient_gid ORDER BY onset_dt DESC, coding_scheme DESC, diag_code) rnum  
    FROM diagnoses
    WHERE diag_type_id IN (31, 32)
  ),
--  select --+ parallel(32) 
--  count(distinct patient_gid) from psychotic_diagnoses;    
--  96,977
  diabetes_prescriptions AS
  (
    SELECT --+ materialize
      patient_gid, MIN(start_dt) start_dt, MAX(stop_dt) stop_dt
    FROM prescriptions
    WHERE drug_type_id = 33 -- Diabetes Prescriptions
    GROUP BY patient_gid
  ),
--  select -- parallel(32)
--  * from diabetes_prescriptions;  
  a1c_glucose_tests AS
  (
    SELECT --+ materialize use_hash(mdm)
      NVL(TO_CHAR(mdm.eid), a1c.network||'-'||a1c.patient_id) patient_gid,
      a1c.network,
      a1c.facility_key,
      a1c.patient_id,
      a1c.test_type_id,
      a1c.visit_id,
      a1c.visit_number,
      a1c.visit_type_id,
      a1c.visit_type,
      a1c.admission_dt,
      a1c.discharge_dt,
      a1c.first_payer_key,
      a1c.result_dt,
--      a1c.data_element_name,
      a1c.result_value,
      ROW_NUMBER() OVER(PARTITION BY NVL(TO_CHAR(mdm.eid), a1c.network||'-'||a1c.patient_id) ORDER BY a1c.result_dt DESC) rnum
    FROM report_dates dt
    JOIN a1c_glucose_rslt a1c
      ON a1c.result_dt >= dt.year_back_dt AND a1c.result_dt < dt.report_dt -- OK: this condition is not probably needed but it does not hurt to have it
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
      ON mdm.network = a1c.network AND mdm.patientid = TO_CHAR(a1c.patient_id) AND mdm.epic_flag = 'N'
  ),
--  select --+ parallel(32) 
--  * from a1c_glucose_tests;
  pcp_info AS
  (
    SELECT --+ materialize
    p.network, 
    NVL(TO_CHAR(mdm.eid), p.network||'-'||p.patient_id) patient_gid, 
    p.pcp_provider_name prim_care_provider,
    v.visit_id, f.facility_name pcp_visit_facility, v.visit_number pcp_visit_number, v.admission_dt pcp_visit_dt,
    ROW_NUMBER() OVER(PARTITION BY p.network, p.patient_id ORDER BY v.admission_dt DESC) rnum 
    FROM dim_patients p
    JOIN fact_visits v ON v.network = p.network AND v.patient_id = p.patient_id AND p.current_flag = 1
    JOIN dim_hc_departments d ON d.department_key = v.last_department_key AND d.service_type = 'PCP'
    LEFT JOIN dim_hc_facilities f ON f.facility_key = v.facility_key
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm 
      ON mdm.network = p.network AND TO_NUMBER(mdm.patientid) = p.patient_id AND p.current_flag = 1 AND mdm.epic_flag = 'N'
  )
--  SELECT --+ parallel(32)
--  * FROM pcp_info;
SELECT --+ USE_HASH(f pd pr) parallel(32)
  dt.report_dt AS report_period_start_dt,
  amed.patient_gid,
  NVL(tst.network, amed.network) network,
  NVL(tst.facility_key, amed.facility_key) facility_key,
  NVL(f.facility_name, 'Unknown') facility_name,
  NVL(tst.patient_id, amed.patient_id) patient_id, 
  pd.name AS patient_name,
  pd.medical_record_number,
  pd.birthdate,
  TRUNC(MONTHS_BETWEEN(dt.report_dt, pd.birthdate)/12) age,
  pd.street_address,
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
  pr.payer_group,
  pr.payer_id,
  pr.payer_name,
  tst.test_type_id,
  tst.result_dt,
--  tst.data_element_name,
  tst.result_value,
  ROW_NUMBER() OVER(PARTITION BY amed.patient_gid, tst.network, tst.visit_id ORDER BY CASE WHEN pr.payer_group = 'Medicaid' THEN 1 ELSE 2 END) rnum  
FROM prescriptions amed
CROSS JOIN report_dates dt
LEFT JOIN pcp_info pcp ON pcp.patient_gid = amed.patient_gid AND pcp.rnum = 1
LEFT JOIN diabetes_diagnoses diab ON diab.patient_gid = amed.patient_gid 
LEFT JOIN psychotic_diagnoses psych ON psych.patient_gid = amed.patient_gid AND psych.rnum = 1
LEFT JOIN diabetes_prescriptions dmed ON dmed.patient_gid = amed.patient_gid
LEFT JOIN a1c_glucose_tests tst ON tst.patient_gid = amed.patient_gid AND tst.rnum = 1
LEFT JOIN dim_patients pd
  ON pd.network = NVL(tst.network, amed.network)
 AND pd.patient_id = NVL(tst.patient_id, amed.patient_id)
 AND pd.current_flag = 1 
LEFT JOIN dim_hc_facilities f --dim_hc_facilities
  ON f.facility_key = amed.facility_key
LEFT JOIN dim_payers pr
  ON pr.payer_key = tst.first_payer_key
WHERE amed.rnum = 1 AND amed.drug_type_id = 34 -- Antipsychotic Medications
AND pd.birthdate > ADD_MONTHS(dt.report_dt, -12*65) -- not 65 yet
AND pd.birthdate <= ADD_MONTHS(dt.report_dt, -12*18) -- 18 or older
AND pd.date_of_death IS NULL
AND
(
  (diab.onset_dt IS NULL OR diab.onset_dt > dt.year_back_dt) -- no Diabetes prior to last year
  AND (dmed.start_dt IS NULL OR dmed.start_dt > dt.year_back_dt) -- no Diabetes Medications taken prior to last year
);



/*
[Error] Execution (1: 1): ORA-12801: error signaled in parallel query server P119
ORA-01652: unable to extend temp segment by 128 in tablespace TEMP
*/

/*
select SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'NETWORK') from dual;
select SYS_CONTEXT('CTX_CDW_MAINTENANCE', 'CLIENT_IDENTIFIER') from dual;

select SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER') from dual;


select count(1) from tst_gk_dsrip_report_tr016;
--137,658

*/