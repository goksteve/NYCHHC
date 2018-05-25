CREATE TABLE dsrip_rpt_pqi90_78_ed_2017
AS
WITH dt
AS 
(
  SELECT --+ materialize
    DATE '2017-12-01' AS report_period_start_dt,
    DATE '2017-01-01' begin_dt,
    DATE '2017-12-01' end_dt
  FROM DUAL
),
visits
AS 
(
  SELECT --+ materialize use_hash(v vsg)
    dt.report_period_start_dt,
    v.network,
    fd.facility_name,
    NVL (REGEXP_SUBSTR (v.visit_number, '^[^-]*'), mdm.mrn) mrn,
    p.patient patient_name,
    INSTR (p.patient, ',') name_comma,
    TRUNC (p.birthdate) patient_dob,
    v.visit_id,
    v.visit_number,
    v.admission_date_time admission_dt,
    v.discharge_date_time discharge_dt,
    p.prim_care_provider,
    v.attending_emp_provider_id,
    v.resident_emp_provider_id,
    v.financial_class_id,
    ROW_NUMBER() OVER (PARTITION BY v.network, v.visit_id ORDER BY mdm.eid) mdm_rnum
  FROM dt
  JOIN cdw.visit v
    ON v.discharge_date_time >= dt.begin_dt AND v.discharge_date_time < dt.end_dt AND v.network NOT IN ('QHN', 'SBN') --AND v.visit_type_id = 1
  LEFT JOIN cdw.visit_segment vsg
    ON vsg.network = v.network AND vsg.visit_id = v.visit_id AND vsg.visit_segment_number =1 --AND vsg.visit_type_id = 2
  LEFT JOIN cdw.hhc_patient_dimension p
    ON p.network = v.network AND p.patient_id = v.patient_id
  LEFT JOIN pt005.facility_dimension fd
    ON fd.network = v.network AND fd.facility_id = v.facility_id
  LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
    ON mdm.network = v.network AND TO_NUMBER (mdm.patientid) = v.patient_id AND SUBSTR (mdm.facility_name, 1, 2) = fd.facility_code
    AND mdm.epic_flag = 'N' AND mdm.dc_flag IS NULL
  WHERE p.birthdate < ADD_MONTHS (v.admission_date_time, -18 * 12)
  AND vsg.visit_type_id = 2
),
--select -- parallel(32)
--* from visits;
payers
AS 
(
  SELECT --+ materialize
    v.network,
    v.visit_id,
    pm.payer_group,
    pm.payer_name,
    ROW_NUMBER () OVER ( PARTITION BY vsp.network, vsp.visit_id ORDER BY CASE WHEN pm.payer_group = 'Medicaid' THEN 1 ELSE 2 END, vsp.payer_number, vsp.visit_segment_number) row_num
  FROM visits v
  JOIN cdw.visit_segment_payer vsp
    ON vsp.network = v.network AND vsp.visit_id = v.visit_id
  JOIN pt008.payer_mapping pm
    ON pm.network = vsp.network AND pm.payer_id = vsp.payer_id
)
SELECT --+ parallel(32)
  v.report_period_start_dt,
  v.network,
  v.facility_name facility,
--  SUBSTR (v.patient_name, 1, name_comma - 1) last_name,
--  SUBSTR (v.patient_name, name_comma + 2) first_name,
  v.patient_name,
  v.patient_dob dob,
  v.mrn,
  v.visit_id,
  v.visit_number,
  v.admission_dt,
  v.discharge_dt,
  numtodsinterval(discharge_dt - add_months(admission_dt, floor(months_between(discharge_dt, admission_dt))), 'day') visit_duration,
  fc.name fin_class,
  insurance_type (p.payer_group) payer_type,
  p.payer_name,
  v.prim_care_provider,
  prv1.name attending_provider,
  prv2.name resident_provider,
  concat_v2_set_gk 
  (
    CURSOR 
    (
      SELECT --+ ordered
        DISTINCT cmv.code || ': ' || cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 38
      JOIN cdw.problem_cmv cmv ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id AND cmv.problem_number = ap.problem_number
        AND cmv.coding_scheme_id = 10 AND cmv.code = meta.VALUE
      WHERE 
      ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10) || '-------------------------------' || CHR (10)
  ) AS hypertension_diagnoses,
  concat_v2_set_gk 
  (
    CURSOR 
    (
      SELECT --+ ordered
        DISTINCT cmv.code || ': ' || cmv.description
      FROM cdw.active_problem ap
      JOIN meta_conditions meta ON meta.criterion_id = 39
      JOIN cdw.problem_cmv cmv ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number AND cmv.coding_scheme_id = 10 AND cmv.code = meta.VALUE
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id
      ORDER BY 1
    ),
    CHR(10) || '-------------------------------' || CHR (10)
  ) AS heart_failure_diagnoses,
  concat_v2_set_gk 
  (
    CURSOR 
    (
      SELECT --+ ordered
        DISTINCT cmv.code || ': ' || cmv.description
      FROM cdw.active_problem ap
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network AND cmv.patient_id = ap.patient_id AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id = 10 AND cmv.code IN ('I12.9', 'I13.10')
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id 
      ORDER BY 1
    ),
    CHR (10) || '-------------------------------' || CHR (10)
  ) AS exclusion_diagnoses
  FROM visits v
  LEFT JOIN cdw.financial_class fc
    ON fc.network = v.network AND fc.financial_class_id = v.financial_class_id
  LEFT JOIN cdw.emp_provider prv1 
    ON prv1.network = v.network AND prv1.emp_provider_id = v.attending_emp_provider_id
  LEFT JOIN cdw.emp_provider prv2
    ON prv1.network = v.network AND prv1.emp_provider_id = v.resident_emp_provider_id
  LEFT JOIN payers p
    ON p.network = v.network AND p.visit_id = v.visit_id AND p.row_num = 1
 WHERE v.mdm_rnum = 1;