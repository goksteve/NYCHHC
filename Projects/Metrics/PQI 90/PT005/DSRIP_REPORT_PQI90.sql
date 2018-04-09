exec dbm.drop_tables('DSRIP_REPORT_PQI90');

CREATE TABLE dsrip_report_pqi90
(
  report_period_start_dt         DATE,
  network                        CHAR(3 BYTE)   NOT NULL,
  facility                       VARCHAR2(100 BYTE),
  last_name                      VARCHAR2(400 BYTE),
  first_name                     VARCHAR2(400 BYTE),
  dob                            DATE,
  mrn                            VARCHAR2(40 CHAR),
  visit_id                       NUMBER(12)     NOT NULL,
  visit_number                   VARCHAR2(40 BYTE),
  admission_dt                   DATE,
  discharge_dt                   DATE,
  fin_class                      VARCHAR2(100 CHAR),
  payer_type                     VARCHAR2(4000 CHAR),
  payer_name                     VARCHAR2(150 BYTE),
  prim_care_provider             VARCHAR2(60 BYTE),
  attending_provider             VARCHAR2(60 CHAR),
  resident_provider              VARCHAR2(60 CHAR),
  diab_shortterm_diagnoses       VARCHAR2(2000 CHAR),
  diab_shortterm_exclusion       VARCHAR2(2000 CHAR),  
  diab_longterm_diagnoses        VARCHAR2(2000 CHAR),
  diab_longterm_exclusion        VARCHAR2(2000 CHAR),        
  copd_asthma_adults_diagnoses   VARCHAR2(2000 CHAR),
  copd_asthma_adults_exclusion   VARCHAR2(2000 CHAR),
  hypertension_diagnoses         VARCHAR2(2000 CHAR),
  hypertension_exclusion         VARCHAR2(2000 CHAR),
  heart_failure_diagnoses        VARCHAR2(2000 CHAR),
  heart_failure_exclusion        VARCHAR2(2000 CHAR),
  dehydration_diagnoses          VARCHAR2(2000 CHAR),
  dehydration_exclusion          VARCHAR2(2000 CHAR),
  bacterial_pneumonia_diagnoses  VARCHAR2(2000 CHAR),
  bacterial_pneumonia_exclusion  VARCHAR2(2000 CHAR),
  urinary_tract_inf_diagnoses    VARCHAR2(2000 CHAR),
  urinary_tract_inf_exclusion    VARCHAR2(2000 CHAR),
  uncontrolled_diab_diagnoses    VARCHAR2(2000 CHAR),
  uncontrolled_diab_exclusion    VARCHAR2(2000 CHAR),
  asthma_yng_adlt_diagnoses      VARCHAR2(2000 CHAR),
  asthma_yng_adlt_exclusion      VARCHAR2(2000 CHAR),
  amputation_diab_diagnoses      VARCHAR2(2000 CHAR),
  amputation_diab_exclusion      VARCHAR2(2000 CHAR),
  CONSTRAINT dsrip_report_pqi90_pk PRIMARY KEY(report_period_start_dt, network, visit_id) USING INDEX COMPRESS
)
COMPRESS BASIC;



INSERT INTO dsrip_report_pqi90
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
      p.patient patient_name,
      INSTR(p.patient, ',') name_comma,
      TRUNC(p.birthdate) patient_dob,
      v.visit_id,
      v.visit_number,
      v.admission_date_time admission_dt,
      v.discharge_date_time discharge_dt,
      p.prim_care_provider,
      v.attending_emp_provider_id,
      v.resident_emp_provider_id,
      v.financial_class_id,
      ROW_NUMBER() OVER(PARTITION BY v.network, v.visit_id ORDER BY mdm.eid) mdm_rnum
    FROM dt
    JOIN cdw.visit v
      ON v.discharge_date_time >= dt.begin_dt AND v.discharge_date_time < dt.end_dt
     AND v.network NOT IN ('QHN','SBN') AND v.visit_type_id = 1
    LEFT JOIN cdw.hhc_patient_dimension p
      ON p.network = v.network AND p.patient_id = v.patient_id
    LEFT JOIN pt005.facility_dimension fd
      ON fd.network = v.network AND fd.facility_id = v.facility_id 
    LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
      ON mdm.network = v.network AND TO_NUMBER(mdm.patientid) = v.patient_id
     AND SUBSTR(mdm.facility_name, 1, 2) = fd.facility_code 
     AND mdm.epic_flag = 'N' AND mdm.dc_flag IS NULL
    WHERE p.birthdate < ADD_MONTHS(v.admission_date_time, -18*12)
  ),
  payers AS
  (
    SELECT --+ materialize
      v.network, v.visit_id,
      pm.payer_group, pm.payer_name,
      ROW_NUMBER() OVER
      (
        PARTITION BY vsp.network, vsp.visit_id
        ORDER BY CASE WHEN pm.payer_group = 'Medicaid' THEN 1 ELSE 2 END, vsp.payer_number, vsp.visit_segment_number
      ) row_num  
    FROM visits v
    JOIN cdw.visit_segment_payer vsp
      ON vsp.network = v.network AND vsp.visit_id = v.visit_id
    JOIN pt008.payer_mapping pm
      ON pm.network = vsp.network AND pm.payer_id = vsp.payer_id      
  )
SELECT
  v.report_period_start_dt,
  v.network, v.facility_name facility,
  SUBSTR(v.patient_name, 1, name_comma-1) last_name,
  SUBSTR(v.patient_name, name_comma+2) first_name,
  v.patient_dob dob, v.mrn,
  v.visit_id, v.visit_number, v.admission_dt, v.discharge_dt,
  fc.name fin_class, Insurance_Type(p.payer_group) payer_type, p.payer_name,
  v.prim_care_provider, prv1.name attending_provider, prv2.name resident_provider,
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
LEFT JOIN cdw.financial_class fc ON fc.network = v.network AND fc.financial_class_id = v.financial_class_id
LEFT JOIN cdw.emp_provider prv1 ON prv1.network = v.network AND prv1.emp_provider_id = v.attending_emp_provider_id
LEFT JOIN cdw.emp_provider prv2 ON prv1.network = v.network AND prv1.emp_provider_id = v.resident_emp_provider_id
LEFT JOIN payers p ON p.network = v.network AND p.visit_id = v.visit_id AND p.row_num = 1
WHERE v.mdm_rnum = 1;

COMMIT;