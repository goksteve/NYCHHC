CREATE TABLE dsrip_tr043_bh_visits_rpt_2017
COMPRESS BASIC
NOLOGGING
AS
WITH 
  bh_vsts AS
  (
    SELECT 
      DISTINCT network, facility_key, patient_id, visit_id, visit_number, admission_dt, discharge_dt, department, vst_provider_key
    FROM 
    (
      SELECT --+ parallel(32)
        *
      FROM 
      (
        SELECT                                     -- use_nl(bh v) matrialize
          v.network, v.facility_key, v.patient_id, v.visit_id, v.visit_number, v.admission_dt, v.discharge_dt, bh.department, v.attending_provider_key, v.resident_provider_key,
          pr1.provider_key emp_provider_key
        FROM fact_visits v
        JOIN dim_hc_departments bh
          ON bh.department_key = v.last_department_key AND bh.service_type = 'BH' AND admission_dt BETWEEN DATE '2017-01-01' AND DATE '2017-12-31' --AND v.visit_id = 25755517 AND v.network = 'GP1'
        LEFT JOIN proc_event_archive pea 
          ON pea.network = v.network AND pea.visit_id = v.visit_id AND pea.emp_provider_id IS NOT NULL
        LEFT JOIN dim_providers pr1
          ON pr1.provider_id = pea.emp_provider_id AND pr1.network = pea.network
      )
      UNPIVOT INCLUDE NULLS
      (
        vst_provider_key FOR provider_type IN (attending_provider_key AS 'ATTENDING', resident_provider_key AS 'RESIDENT', emp_provider_key AS 'EMP_PROVIDER_ID')
      )
    )
  ),
  bh_visit_info AS
  (  
    SELECT --+ ordered use_nl(pd)
        v.network, v.facility_key, v.patient_id, v.visit_id, v.visit_number, v.admission_dt, v.discharge_dt, v.department, 
		MIN(pr.provider_name ||' - '|| pr.physician_service_name_1) KEEP (DENSE_RANK FIRST ORDER BY CASE WHEN UPPER(pr.physician_service_name_1) LIKE '%PSYCH%' THEN 1 WHEN pr.physician_service_name_1 IS NOT NULL THEN 2 ELSE 3 END) bh_provider_info
    FROM bh_vsts v
    LEFT JOIN dim_providers pr
      ON pr.provider_key = v.vst_provider_key 
    GROUP BY v.network, v.facility_key, v.patient_id, v.visit_id, v.visit_number, v.admission_dt, v.discharge_dt, v.department
  )

SELECT --+ parallel(32) ordered()
  v.network,
  p.patient_id,
  p.name AS patient_name,
  p.birthdate AS dob,
  v.visit_id,
  v.visit_number,
  nvl(p.medical_record_number, mdm.mrn) mrn,
  concat_v2_set_gk
  (
    CURSOR
    (
      SELECT -- index(ap pk_fact_visit_diag)
        DISTINCT ap.icd_code||': '||ap.problem_comments
        FROM fact_visit_diagnoses ap
      WHERE ap.network = v.network AND ap.visit_id = v.visit_id AND ap.coding_scheme = 'ICD-10'
    ),
    CHR(10)||'-------------------------------'||CHR(10)
  ) diagnoses, 
  v.department,
  TRUNC( MONTHS_BETWEEN(v.admission_dt, p.birthdate)/12) AS age,
  p.street_address,
  p.apt_suite,
  p.city,
  p.state,
  p.country,
  p.mailing_code zip_code,
  p.home_phone,
  p.day_phone cell_phone,
  p.pcp_provider_name pcp_general_med,
  substr(v.bh_provider_info, 1, instr(v.bh_provider_info,'-') -2) visit_provider_name,
  substr(v.bh_provider_info, instr(v.bh_provider_info,'-')+2) visit_provider_splty,
--  v.bh_provider_info AS visit_provider,
  f.facility_name,
  v.admission_dt,
  v.discharge_dt
FROM bh_visit_info v
JOIN dim_patients p ON p.patient_id = v.patient_id AND p.network = v.network AND p.current_flag = 1
JOIN dim_hc_facilities f ON f.facility_key = v.facility_key
LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
  ON mdm.network = v.network AND TO_NUMBER(mdm.patientid) = v.patient_id AND mdm.epic_flag = 'N' AND f.facility_name = v.facility_name;