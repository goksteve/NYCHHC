CREATE OR REPLACE FORCE VIEW pt005.v_dsrip_report_tr001_qmed_n
AS
WITH 
-- 16-Apr-2018, GK:Fixed No records output issue caused by missiong hyphen '-' in ICD9, ICD10 strings
-- 02-Apr-2018, SS:Fixed MRN issue caused by mdm.dc_flag
-- 07-Mar-2018, OK: created
  dt AS 
  (
    SELECT --+ materialize
      mon AS report_period_start_dt,
      ADD_MONTHS (mon, -2) begin_dt,
      ADD_MONTHS (mon, -1) end_dt
    FROM 
    (
      SELECT TRUNC (NVL (TO_DATE (SYS_CONTEXT ('USERENV', 'CLIENT_IDENTIFIER')),SYSDATE), 'MONTH') mon FROM DUAL)
    ),
  visits AS 
  (
    SELECT /*+ materialize */
    *
    FROM 
    (                                   -- Visits with BH Diagnoses:
      SELECT --+ ordered
        dt.report_period_start_dt,
        vst.network,
        vst.facility_id,
        vst.patient_id,
        vst.visit_id,
        vst.visit_number,
        REGEXP_SUBSTR (vst.visit_number, '^[^-]*') mrn,
        vt.abbreviation visit_type_cd,
        vst.financial_class_id,
        vst.admission_date_time admission_dt,
        vst.discharge_date_time discharge_dt,
        vst.attending_emp_provider_id,
        vst.resident_emp_provider_id
      FROM dt
      JOIN cdw.visit vst
        ON (vst.visit_type_id = 1   -- 1-Inpatient(IP)
       AND vst.discharge_date_time >= dt.begin_dt
       AND vst.discharge_date_time < dt.end_dt
       OR vst.visit_type_id IN (3, 4) -- 3-Outpatient(OP), 4-Clinic(CP)
       AND vst.admission_date_time >= dt.begin_dt)
      JOIN cdw.active_problem ap
        ON ap.network = vst.network
       AND ap.visit_id = vst.visit_id
      JOIN cdw.problem_cmv cmv
        ON cmv.network = ap.network
       AND cmv.patient_id = ap.patient_id
       AND cmv.problem_number = ap.problem_number
       AND cmv.coding_scheme_id IN (5, 10)
      JOIN meta_conditions mc
        ON mc.qualifier = DECODE (cmv.coding_scheme_id,5, 'ICD-9','ICD-10')
       AND mc.VALUE = cmv.code
       AND mc.criterion_id = 9 -- DIAGNOSES:MENTAL HEALTH
      JOIN cdw.ref_visit_types vt
        ON vt.visit_type_id = vst.visit_type_id
      UNION
      SELECT --+ ordered
        dt.report_period_start_dt,
        vst.network,
        vst.facility_id,
        vst.patient_id,
        vst.visit_id,
        vst.visit_number,
        REGEXP_SUBSTR (vst.visit_number, '^[^-]*') mrn,
        vt.abbreviation visit_type_cd,
        vst.financial_class_id,
        vst.admission_date_time admission_dt,
        vst.discharge_date_time discharge_dt,
        vst.attending_emp_provider_id,
        vst.resident_emp_provider_id
      FROM dt
      JOIN cdw.visit vst
        ON vst.visit_type_id IN (3, 4) -- 3-Outpatient(OP), 4-Clinic(CP)
       AND vst.admission_date_time >= dt.begin_dt
      JOIN cdw.ref_visit_types vt
        ON vt.visit_type_id = vst.visit_type_id
      JOIN cdw.visit_segment_visit_location vl
        ON     vl.network = vst.network
       AND vl.visit_id = vst.visit_id
      JOIN cdw.dim_hc_departments dp
        ON     dp.network = vl.network
       AND dp.location_id = vl.location_id
       AND dp.service_type = 'BH'
     ) q
  ),
 visit_providers AS 
 (
  SELECT /*+ materialize */
    *
  FROM 
  (
    SELECT network,
                      visit_id,
                      attending_emp_provider_id AS provider_id
                 FROM visits
                WHERE attending_emp_provider_id IS NOT NULL
               UNION
               SELECT network,
                      visit_id,
                      resident_emp_provider_id AS provider_id
                 FROM visits
                WHERE resident_emp_provider_id IS NOT NULL
               UNION
               SELECT vst.network,
                      vst.visit_id,
                      pea.emp_provider_id AS provider_id
                 FROM visits vst
                      JOIN cdw.proc_event_archive pea
                         ON     pea.network = vst.network
                            AND pea.visit_id = vst.visit_id
                            AND pea.emp_provider_id IS NOT NULL)
  )
SELECT --+ parallel(32)
  report_period_start_dt,
  network,
  SUBSTR (patient_name, 1, name_comma - 1) last_name,
  SUBSTR (patient_name, name_comma + 2) first_name,
  patient_dob dob,
  streetadr,
  apt_suite,
  city,
  state,
  zipcode,
  country,
  home_phone,
  day_phone,
  prim_care_provider,
  visit_id,
  hospitalization_facility,
  mrn,
  visit_number,
  admission_dt,
  discharge_dt,
  follow_up_visit_id,
  follow_up_facility,
  follow_up_visit_number,
  follow_up_dt,
  bh_provider_info,
  payer,
  insurance_type (payer_group) payer_group,
  follow_up_fin_class fin_class,
  follow_up_30_days,
  follow_up_7_days
FROM 
(
  SELECT 
    q.report_period_start_dt,
    q.network,
    q.patient_name,
    INSTR (q.patient_name, ',') name_comma,
    TRUNC (q.patient_dob) patient_dob,
    q.streetadr,
    q.apt_suite,
    q.city,
    q.state,
    q.zipcode,
    q.country,
    q.home_phone,
    q.day_phone,
    q.mrn,
    q.visit_id,
    q.visit_number,
    q.prim_care_provider,
    q.facility_name hospitalization_facility,
    q.admission_dt,
    q.discharge_dt,
    q.follow_up_visit_id,
    q.follow_up_visit_number,
    q.follow_up_dt,
    q.follow_up_facility,
    prv.provider_name || ' - ' || prv.physician_service_name
    bh_provider_info,
    ROW_NUMBER() OVER 
    (
      PARTITION BY q.network, q.visit_id ORDER BY
      CASE
        WHEN UPPER (prv.physician_service_name) LIKE '%PSYCH%'
        THEN 1
        WHEN prv.physician_service_name IS NOT NULL
        THEN 2
        ELSE 3
      END, provider_name
    ) AS provider_rnum,
    q.follow_up_fin_class,
    CASE WHEN q.follow_up_dt < q.discharge_dt + 30 THEN 'Y' END follow_up_30_days,
    CASE WHEN q.follow_up_dt < q.discharge_dt + 7 THEN 'Y' END follow_up_7_days,
    pyr.payer_name payer,
    pyr.payer_group,
    ROW_NUMBER() OVER 
    (
      PARTITION BY q.network, q.visit_id
      ORDER BY
        CASE
          WHEN pyr.payer_group = 'Medicaid' THEN 1
          ELSE 2
        END, vsp.payer_number
    ) AS payer_rnum
  FROM 
  (
    SELECT 
      report_period_start_dt,
      network,
      facility_id,
      facility_name,
      patient_id,
      mrn,
      patient_name,
      patient_dob,
      prim_care_provider,
      streetadr,
      apt_suite,
      city,
      state,
      zipcode,
      country,
      home_phone,
      day_phone,
      visit_id,
      visit_number,
      visit_type_cd,
      fin_class,
      TRUNC(admission_dt) admission_dt,
      TRUNC(discharge_dt) discharge_dt,
      LEAD(TRUNC(re_admission_dt) IGNORE NULLS) OVER (PARTITION BY patient_gid ORDER BY admission_dt) AS re_admission_dt,
      LEAD(TRUNC(follow_up_dt) IGNORE NULLS) OVER (PARTITION BY patient_gid ORDER BY admission_dt) AS follow_up_dt,
      LEAD(follow_up_visit_id IGNORE NULLS) OVER (PARTITION BY patient_gid ORDER BY admission_dt) AS follow_up_visit_id,
      LEAD(follow_up_visit_number IGNORE NULLS) OVER (PARTITION BY patient_gid ORDER BY admission_dt)AS follow_up_visit_number,
      LEAD(follow_up_facility IGNORE NULLS) OVER (PARTITION BY patient_gid ORDER BY admission_dt) AS follow_up_facility,
      LEAD(follow_up_fin_class IGNORE NULLS) OVER (PARTITION BY patient_gid ORDER BY admission_dt) AS follow_up_fin_class
    FROM 
    (
      SELECT 
        vst.report_period_start_dt,
        vst.network,
        vst.facility_id,
        fd.facility_name,
        vst.patient_id,
        CASE
          WHEN mdm.onmlast IS NOT NULL AND mdm.onmfirst IS NOT NULL
          THEN mdm.onmlast || ', ' || mdm.onmfirst
          ELSE pat.name
        END AS patient_name,
        NVL (TO_DATE (mdm.dob, 'YYYY-MM-DD'), pat.birthdate) AS patient_dob,
        NVL (vst.mrn, mdm.mrn) AS mrn,
        mdm.streetadr,
        mdm.apt_suite,
        mdm.city,
        mdm.state,
        mdm.zipcode,
        mdm.country,
        mdm.home_phone,
        mdm.day_phone,
        NVL(TO_CHAR (mdm.eid), vst.network || '_' || vst.patient_id) AS patient_gid,
        pat.pcp_provider_name AS prim_care_provider,
        vst.visit_id,
        vst.visit_number,
        vst.admission_dt,
        vst.discharge_dt,
        vst.visit_type_cd,
        fc.name AS fin_class,
        CASE
          WHEN vst.visit_type_cd = 'IP'
          THEN vst.admission_dt
        END AS re_admission_dt,
        CASE
          WHEN vst.visit_type_cd <> 'IP'
          THEN vst.visit_id
        END AS follow_up_visit_id,
        CASE
          WHEN vst.visit_type_cd <> 'IP'
          THEN vst.visit_number
        END AS follow_up_visit_number,
        CASE
          WHEN vst.visit_type_cd <> 'IP'
          THEN vst.admission_dt
        END AS follow_up_dt,
        CASE
          WHEN vst.visit_type_cd <> 'IP'
          THEN fd.facility_name
        END AS follow_up_facility,
        CASE
          WHEN vst.visit_type_cd <> 'IP'
          THEN fc.name
        END AS follow_up_fin_class,
        ROW_NUMBER() OVER 
        (
          PARTITION BY vst.network, vst.visit_id
          ORDER BY
          CASE
            WHEN SUBSTR ( mdm.facility_name,1,2) = fd.facility_cd AND mdm.mrn = vst.mrn
            THEN 1
            WHEN SUBSTR (mdm.facility_name, 1,2) = fd.facility_cd
            THEN 2
          ELSE 3
          END, eid
        ) AS mdm_rnum
      FROM visits vst
      JOIN cdw.financial_class fc
        ON fc.network = vst.network
       AND fc.financial_class_id = vst.financial_class_id
      JOIN cdw.dim_hc_facilities fd
        ON fd.network = vst.network
       AND fd.facility_id = vst.facility_id
      JOIN cdw.dim_patients pat
        ON pat.network = vst.network
       AND pat.patient_id = vst.patient_id
      LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
        ON mdm.network = vst.network
       AND TO_NUMBER (mdm.patientid) = vst.patient_id
      AND mdm.epic_flag = 'N' --AND mdm.dc_flag IS NULL
    )
    WHERE mdm_rnum = 1
  ) q
  LEFT JOIN cdw.visit_segment_payer vsp
    ON vsp.network = q.network
   AND vsp.visit_id = NVL(q.follow_up_visit_id, q.visit_id)
  LEFT JOIN cdw.dim_payers pyr
    ON pyr.network = vsp.network
   AND pyr.payer_id = vsp.payer_id
  LEFT JOIN visit_providers vpr
    ON vpr.network = q.network
   AND vpr.visit_id = NVL (q.follow_up_visit_id, q.visit_id)
  LEFT JOIN cdw.dim_providers prv
    ON prv.network = vpr.network AND prv.provider_id = vpr.provider_id
  WHERE q.visit_type_cd = 'IP'
  AND q.patient_dob <= ADD_MONTHS (q.discharge_dt, -72) -- 6 years or older as of discharge date
  AND( q.re_admission_dt IS NULL OR q.re_admission_dt >= q.discharge_dt + 30) -- ignore IP Visits with re-admissions
  )
WHERE payer_rnum = 1 AND provider_rnum = 1;