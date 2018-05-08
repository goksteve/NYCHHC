CREATE OR REPLACE VIEW v_dsrip_report_pqi90
AS
WITH
  dt AS
  (
    SELECT --+ materialize
      mon AS report_period_start_dt,
      ADD_MONTHS(mon, -1) begin_dt,
      mon end_dt
    FROM
    (
      SELECT TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH') mon
      FROM dual
    )
  ),
  visits AS
  (
    SELECT --+ materialize
      dt.report_period_start_dt,
      v.network,
      fd.facility_name,
      NVL(REGEXP_SUBSTR(v.visit_number, '^[^-]*'), mdm.mrn) mrn,
      v.patient_id,
      p.name patient_name,
      TRUNC(p.birthdate) patient_dob,
      p.street_address,
      p.apt_suite,
      p.city,
      p.state,
      p.country,
      p.mailing_code zip_code,
      p.home_phone,
      p.day_phone cell_phone,
      v.visit_id,
      v.visit_number,
      v.admission_dt,
      v.discharge_dt,
      p.pcp_provider_name prim_care_provider,
      prv1.provider_name attending_provider,
      prv2.provider_name resident_provider,
      fc.financial_class_name AS fin_class,
      pm.payer_group,
      pm.payer_name,
      ROW_NUMBER() OVER(PARTITION BY v.network, v.visit_id ORDER BY mdm.eid) mdm_rnum
    FROM dt
    JOIN fact_visits v
      ON v.discharge_dt >= dt.begin_dt AND v.discharge_dt < dt.end_dt
     AND v.network NOT IN ('QHN','SBN') AND v.final_visit_type_id = 1
    LEFT JOIN /*cdw.hhc_patient_dimension p*/ dim_patients p
      ON p.network = v.network AND p.patient_id = v.patient_id AND p.current_flag = 1
    LEFT JOIN /*pt005.facility_dimension fd*/ dim_hc_facilities fd
      ON fd.facility_key = v.facility_key 
    LEFT JOIN dim_providers prv1
      ON prv1.provider_key = v.admitting_provider_key
    LEFT JOIN dim_providers prv2
      ON prv2.provider_key = v.resident_provider_key    
    LEFT JOIN ref_financial_class fc  
      ON fc.financial_class_id = v.financial_class_id AND fc.network = v.network
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
      ON mdm.network = v.network AND TO_NUMBER(mdm.patientid) = v.patient_id
     AND SUBSTR(mdm.facility_name, 1, 2) = fd.facility_cd 
     AND mdm.epic_flag = 'N' AND mdm.dc_flag IS NULL
    LEFT JOIN dim_payers pm
      ON pm.payer_key = v.first_payer_key
    WHERE p.birthdate < ADD_MONTHS(v.admission_dt, -18*12)
  ),
  pcp_info AS
  (
    SELECT --+ materialize
    v.network, v.patient_id, v.visit_id pcp_visit_id, v.admission_dt pcp_visit_dt, fd.facility_name AS pcp_vst_facility_name,
    ROW_NUMBER() OVER(PARTITION BY v.network, v.patient_id ORDER BY v.admission_dt DESC) rnum 
    FROM visits vst
    JOIN cdw.fact_visits v ON v.network = vst.network  AND v.patient_id = vst.patient_id 
    JOIN cdw.dim_hc_departments d ON d.department_key = v.last_department_key AND d.service_type = 'PCP'
    JOIN dim_hc_facilities fd
      ON fd.facility_key = v.facility_key 
  )  
SELECT -- parallel(16)
  v.report_period_start_dt,
  v.network, v.facility_name facility,
  SUBSTR(v.patient_name, 1, INSTR(v.patient_name, ',', 1) - 1) AS last_name,
  SUBSTR(v.patient_name, INSTR(v.patient_name, ',') + 1) AS first_name,  
  v.patient_dob dob, v.mrn,
  v.street_address, v.apt_suite, v.city, v.state, v.country, v.zip_code, v.home_phone, v.cell_phone,
  v.visit_id, v.visit_number, v.admission_dt, v.discharge_dt,
  v.fin_class, 
  CASE
    WHEN v.payer_group IN ('Medicaid', 'Medicare') OR v.payer_group IS NULL THEN payer_group
    WHEN v.payer_group = 'UNINSURED' THEN 'Self pay'
    ELSE 'Commercial'
  END payer_type, 
  v.payer_name,
  v.prim_care_provider, v.attending_provider, v.resident_provider,
  pcp.pcp_visit_id,
  pcp.pcp_visit_dt,
  pcp.pcp_vst_facility_name,
--DIAGNOSES:DIABETES SHORT TERM COMPLICATIONS:PQI90-1, criterion_id=50
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 50
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) diab_shortterm_diagnoses,
  NULL diab_shortterm_exclusion,
  
--DIAGNOSES:DIABETES LONG TERM COMPLICATIONS:PQI90-3, criterion_id=51
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 51
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) diab_longterm_diagnoses,
  NULL diab_longterm_exclusion,

--DIAGNOSES:(COPD) OR ASTHMA OLDER ADLTS ADMRATE:PQI90-5, criterion_id=53
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 53
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) copd_asthma_adults_diagnoses,
  NULL copd_asthma_adults_exclusion,



--DIAGNOSES:HYPERTENSION:PQI90-7, criterion_id=38
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 38
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND cmv.code = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) hypertension_diagnoses,
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN cdw.problem_cmv cmv ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND cmv.code IN ('I12.9','I13.10')
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) hypertension_exclusion,
  
--DIAGNOSES:HEART FAILURE:PQI90-8, criterion_id=39
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 39
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND cmv.code = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) heart_failure_diagnoses,
  NULL heart_failure_exclusion,

--DIAGNOSES:DEHYDRATION:PQI90-10, criterion_id = 54
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 54 AND include_exclude_ind = 'I'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND cmv.code = meta.value 
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) dehydration_diagnoses,
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 54 AND include_exclude_ind = 'E'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND cmv.code = meta.value 
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) dehydration_exclusion,


--DIAGNOSES:BACTERIAL PNEUMONIA:PQI90-11, criterion_id = 55
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 55 AND include_exclude_ind = 'I'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) bacterial_pneumonia_diagnoses,
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 55 AND include_exclude_ind = 'E'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) bacterial_pneumonia_exclusion,

--DIAGNOSES:URINARY TRACT INFECTION:PQI90-12, criterion_id = 56
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 56 AND include_exclude_ind = 'I'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) urinary_tract_inf_diagnoses,
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 56 AND include_exclude_ind = 'E'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) urinary_tract_inf_exclusion,

--DIAGNOSES:UNCONTROLLED DIABETES:PQI90-14, criterion_id = 52
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 52 AND include_exclude_ind = 'I'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) uncontrolled_diab_diagnoses,
  NULL uncontrolled_diab_exclusion,

--DIAGNOSES:ASTHMA IN YOUND ADULTS:PQI90-15, cirterion_id = 57
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 57 AND include_exclude_ind = 'I'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) asthma_yng_adlt_diagnoses,
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 57 AND include_exclude_ind = 'E'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) asthma_yng_adlt_exclusion,
  
--DIAGNOSES:LOWER-EXTREMITY AMPUTATION WITH DIAB:PQI90-16, criterion_id = 58  
  concat_v2_set
  (
    CURSOR
    (
      SELECT --+ ordered
        DISTINCT cmv.code||': '||cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 58 AND include_exclude_ind = 'I'
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND REPLACE(cmv.code,'.','') = meta.value
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) amputation_diab_diagnoses,
  NULL amputation_diab_exclusion
FROM visits v
LEFT JOIN pcp_info pcp ON pcp.network = v.network AND pcp.patient_id = v.patient_id AND pcp.rnum = 1
WHERE v.mdm_rnum = 1;