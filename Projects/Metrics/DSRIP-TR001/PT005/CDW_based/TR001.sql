prompt Populating DSRIP_TR001_VISITS ...

ALTER SESSION ENABLE PARALLEL DML;

TRUNCATE TABLE dsrip_tr001_visits;

INSERT --+ parallel(4) 
INTO dsrip_tr001_visits
WITH
  dt AS
  (
    SELECT --+ materialize
      network,
      mon AS report_period_start_dt,
      ADD_MONTHS(mon, -2) begin_dt,
      ADD_MONTHS(mon, -1) end_dt
    FROM
    (
      SELECT
        SUBSTR(name, 1, 3) network,
        TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH') mon
      FROM v$database
    )
  );
SELECT
  q.report_period_start_dt,
  q.network,
  q.facility_id,
  q.patient_id,
  p.patient patient_name,
  TRUNC(p.birthdate) patient_dob,
  p.prim_care_provider,
  q.visit_id,
  q.visit_number,
  REGEXP_SUBSTR(q.visit_number, '^[^-]*') mrn,
  q.admission_dt,
  q.discharge_dt,
  q.visit_type_cd,
  fc.name fin_class,
  q.attending_emp_provider_id,
  q.resident_emp_provider_id
FROM
(
  SELECT
    dt.report_period_start_dt,
    dt.network,
    v.facility_id,
    v.patient_id,
    v.visit_id,
    v.visit_number,
    vt.abbreviation visit_type_cd,
    v.financial_class_id,
    v.admission_date_time admission_dt,
    v.discharge_date_time discharge_dt,
    v.attending_emp_provider_id,
    v.resident_emp_provider_id
  FROM dt
  JOIN cdw.visit v
    ON v.discharge_date_time >= dt.begin_dt
--   AND v.discharge_date_time < dt.end_dt -- this condition is commented-out because we need 'OP' and 'CP' Visits up to the current date 
  JOIN cdw.ref_visit_types vt ON vt.visit_type_id = v.visit_type_id AND vt.abbreviation IN ('IP','OP','CP')
  JOIN cdw.active_problem ap ON ap.network = v.network AND ap.visit_id = v.visit_id
  JOIN cdw.problem prob ON prob.network = ap.network AND prob.patient_id = ap.patient_id AND prob.problem_number = ap.problem_number
  JOIN cdw.problem_icd_diagnosis pid ON pid.network = prob.network AND pid.patient_id = prob.patient_id AND pid.problem_number = ap.problem_number
  JOIN pt005.ref_hedis_value_sets vs ON vs.code = pid.icd_diagnosis_code AND vs.value_set_name = 'Mental Illness'
 UNION
  SELECT
    dt.report_period_start_dt,
    dt.network,
    v.facility_id, 
    v.patient_id,
    v.visit_id,
    v.visit_number,
    vt.abbreviation visit_type_cd, 
    v.financial_class_id,
    v.admission_date_time admission_dt,
    v.discharge_date_time discharge_dt,
    v.attending_emp_provider_id,
    v.resident_emp_provider_id
  FROM dt
  JOIN cdw.visit v ON v.admission_date_time >= dt.begin_dt  
  JOIN cdw.ref_visit_types vt ON vt.visit_type_id = v.visit_type_id AND vt.abbreviation IN ('OP','CP')
  JOIN cdw.visit_segment_visit_location vl ON vl.network = v.network AND vl.visit_id = v.visit_id
  JOIN cdw.hhc_location_dimension ld ON ld.network = vl.network AND ld.location_id = vl.location_id
  JOIN cdw.hhc_clinic_codes cc ON cc.cc.code = ld.clinic_code AND cc.service = 'Mental Health'
) q
JOIN cdw.hhc_patient_dimension p ON p.patient_id = q.patient_id
JOIN cdw.financial_class fc ON fc.financial_class_id = q.financial_class_id;

COMMIT;