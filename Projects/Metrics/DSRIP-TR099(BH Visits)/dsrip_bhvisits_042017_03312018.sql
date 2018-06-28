--CREATE OR REPLACE FORCE VIEW cdw.v_dsrip_tr043_bh_visits_report
CREATE TABLE dsrip_bhvisits_042017_03312018
NOLOGGING
PARALLEL 32
AS
/* Formatted on 6/28/2018 10:02:12 (QP5 v5.287) */
WITH bh_vsts
  AS 
  (
    SELECT --+ materialize
      v.*
    FROM cdw.fact_visits v
    JOIN cdw.dim_hc_departments bh
      ON bh.department_key = v.last_department_key
     AND bh.service_type = 'BH'
     AND admission_dt >= date '2017-04-01'  AND admission_dt < date '2018-04-01'
  ),
  providers AS 
  (
    SELECT --+ materialize
      network, visit_id, attending_provider_key AS provider_id
    FROM bh_vsts
    WHERE attending_provider_key IS NOT NULL                      --1072
    
    UNION
    
    SELECT 
      network, visit_id, resident_provider_key AS provider_id
    FROM bh_vsts
    WHERE resident_provider_key IS NOT NULL                          --2
    
    UNION
    
    SELECT 
      DISTINCT v.network, v.visit_id, pea.emp_provider_id AS provider_id
    FROM bh_vsts v
    JOIN cdw.proc_event_archive pea
      ON v.visit_id = pea.visit_id
     AND v.network = pea.network
     AND pea.emp_provider_id IS NOT NULL             --11,740
  ),
  bh_provider AS 
  (  
    SELECT --+  materialize --ordered use_nl(pd)
      pr.network, pr.visit_id,
      MIN(pd.provider_name || ' : ' || pd.physician_service_name_1) 
        KEEP 
        (
          DENSE_RANK FIRST 
          ORDER BY 
          CASE
            WHEN UPPER (pd.physician_service_name_1) LIKE '%PSYCH%'
            THEN 1
            WHEN pd.physician_service_name IS NOT NULL
            THEN 2
            ELSE 3
          END
        ) AS bh_provider_info
    FROM providers pr
    JOIN dim_providers pd
      ON pd.provider_id = pr.provider_id
     AND pd.network = pr.network
    GROUP BY pr.network, pr.visit_id
  )
SELECT --+ parallel(32)
--  SYS_CONTEXT ('USERENV', 'CLIENT_IDENTIFIER') AS report_period_dt,
  '01-Apr-2017 TO 31-Mar-2018' AS msrmnt_period,
  v.network,
  p.name AS patient_name,
  p.birthdate AS dob,
  v.visit_id,
  v.visit_number,
  NVL(mdm.mrn, p.medical_record_number) mrn,
  TRUNC (MONTHS_BETWEEN (v.admission_dt, p.birthdate) / 12) AS age,
  p.street_address,
  p.apt_suite,
  p.city,
  p.state,
  p.country,
  p.mailing_code AS zip_code,
  p.home_phone,
  p.day_phone AS cell_phone,
  p.pcp_provider_name AS pcp_general_med,
  SUBSTR (bh_provider_info, 1, INSTR (bh_provider_info, ':') - 2) AS visit_provider_name,
  SUBSTR (bh_provider_info, INSTR (bh_provider_info, ':') + 2) AS visit_provider_splty,
  f.facility_name,
  v.admission_dt,
  v.discharge_dt
FROM bh_vsts v
JOIN cdw.dim_patients p
  ON p.patient_id = v.patient_id
 AND p.network = v.network
 AND p.current_flag = 1
JOIN cdw.dim_hc_facilities f 
  ON f.facility_key = v.facility_key
LEFT JOIN bh_provider b
  ON b.visit_id = v.visit_id AND b.network = v.network
LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
  ON mdm.network = v.network
 AND TO_NUMBER (mdm.patientid) = v.patient_id
 AND mdm.epic_flag = 'N'
 AND f.facility_name = mdm.facility_name;