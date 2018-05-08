DROP TABLE dsrip_tr043_bh_visits_report purge;

CREATE TABLE dsrip_tr043_bh_visits_report
NOLOGGING
COMPRESS BASIC
AS
WITH
  bh_vsts AS
  (
    SELECT v.*
    FROM fact_visits v
    JOIN dim_hc_departments bh
      ON bh.department_key = v.last_department_key 
     AND bh.service_type='BH' 
     AND admission_dt BETWEEN ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)  AND (TRUNC(SYSDATE, 'MONTH') - 1)
  ),
  providers AS
  (
    SELECT --+ parallel(32) 
    network, visit_id, attending_provider_key AS provider_id
     FROM bh_vsts
     WHERE attending_provider_key IS NOT NULL --1072
    UNION 
     SELECT --+ parallel(32)  
     network, visit_id, resident_provider_key AS provider_id
     FROM bh_vsts
     WHERE resident_provider_key IS NOT NULL --2
    UNION
     SELECT --+ parallel(32)   -- noparallel index(pea) 
       distinct v.network, v.visit_id, pea.emp_provider_id AS provider_id
     FROM bh_vsts v
     JOIN proc_event_archive pea
      ON v.visit_id = pea.visit_id AND v.network = pea.network AND pea.emp_provider_id IS NOT NULL --11,740
  ),
  bh_provider AS
  (  
    SELECT --+ ordered use_nl(pd)
        pr.network, pr.visit_id, MIN(pd.provider_name||' - '||pd.physician_service_name_1) KEEP (DENSE_RANK FIRST ORDER BY CASE WHEN UPPER(pd.physician_service_name_1) LIKE '%PSYCH%' THEN 1 WHEN pd.physician_service_name IS NOT NULL THEN 2 ELSE 3 END) bh_provider_info
    FROM providers pr
    JOIN dim_providers pd
      ON pd.provider_id = pr.provider_id AND pd.network = pr.network
    GROUP BY pr.network, pr.visit_id
  )
--  select * from bh_provider;
SELECT --+ parallel(32)
  trunc(sysdate, 'MONTH') report_period_dt,
  p.name AS patient_name,
  p.birthdate AS dob,
  v.visit_id,
  v.visit_number,
  p.medical_record_number mrn,
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
  substr(bh_provider_info, 1, instr(bh_provider_info,'-') -2) visit_provider_name,
  substr(bh_provider_info, instr(bh_provider_info,'-')+2) visit_provider_splty,
--  b.bh_provider_info AS visit_provider,
  f.facility_name,
  v.admission_dt,
  v.discharge_dt
FROM bh_vsts v
JOIN dim_patients p ON p.patient_id = v.patient_id AND p.network = v.network AND p.current_flag = 1
JOIN dim_hc_facilities f ON f.facility_key = v.facility_key
LEFT JOIN bh_provider b ON b.visit_id = v.visit_id AND b.network = v.network; 