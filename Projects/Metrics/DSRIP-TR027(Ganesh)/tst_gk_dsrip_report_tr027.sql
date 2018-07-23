CREATE TABLE tst_gk_dsrip_report_tr027
NOLOGGING
PARALLEL 32
AS
WITH
  dt AS
  (
    SELECT --+ materialize
      TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH') AS report_dt,
      ADD_MONTHS (TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH'), -24) AS begin_dt
    FROM dual   
  ),
msrmnt_period_ptnts AS
(
  SELECT --+ materialize
    ptnt_diag.diag_code, ptnt_diag.diag_coding_scheme, ptnt_diag.network, ptnt_diag.onset_date, ptnt_diag.patient_id, ptnt_diag.problem_comments,
    ROW_NUMBER() OVER (PARTITION BY ptnt_diag.network, ptnt_diag.patient_id ORDER BY onset_date ASC) AS prblm_onst_dt_rnum  
  FROM dt  
  JOIN fact_patient_diagnoses  ptnt_diag
    ON ptnt_diag.onset_date >= dt.begin_dt AND ptnt_diag.onset_date < dt.report_dt  
  JOIN TABLE 
  (
    tab_v256 
    (
      'F10.10','F10.120','F10.121','F10.129','F10.14','F10.150','F10.151','F10.159','F10.180','F10.181','F10.182','F10.188','F10.19','F10.20','F10.220','F10.221','F10.229','F10.230','F10.231','F10.232','F10.239','F10.24','F10.250','F10.251','F10.259','F10.26','F10.27','F10.280','F10.281','F10.282','F10.288','F10.29','F11.10','F11.120','F11.121','F11.122','F11.129','F11.14','F11.150','F11.151','F11.159','F11.181','F11.182','F11.188','F11.19','F11.20','F11.220','F11.221','F11.222','F11.229','F11.23','F11.24','F11.250','F11.251','F11.259','F11.281','F11.282','F11.288','F11.29','F12.10','F12.120','F12.121','F12.122','F12.129','F12.150','F12.151','F12.159','F12.180','F12.188','F12.19','F12.20','F12.220','F12.221','F12.222','F12.229','F12.250','F12.251','F12.259','F12.280','F12.288','F12.29','F13.10','F13.120','F13.121','F13.129','F13.14','F13.150','F13.151','F13.159','F13.180','F13.181','F13.182','F13.188','F13.19','F13.20','F13.220','F13.221','F13.229','F13.230','F13.231','F13.232','F13.239','F13.24','F13.250','F13.251','F13.259','F13.26','F13.27','F13.280','F13.281','F13.282','F13.288','F13.29','F14.10','F14.120','F14.121','F14.122','F14.129','F14.14','F14.150','F14.151','F14.159','F14.180','F14.181','F14.182','F14.188','F14.19','F14.20','F14.220','F14.221','F14.222','F14.229','F14.23','F14.24','F14.250','F14.251','F14.259','F14.280','F14.281','F14.282','F14.288','F14.29','F15.10','F15.120','F15.121','F15.122','F15.129','F15.14','F15.150','F15.151','F15.159','F15.180','F15.181','F15.182','F15.188','F15.19','F15.20','F15.220','F15.221','F15.222','F15.229','F15.23','F15.24','F15.250','F15.251','F15.259','F15.280','F15.281','F15.282','F15.288','F15.29','F16.10','F16.120','F16.121','F16.122','F16.129','F16.14','F16.150','F16.151','F16.159','F16.180','F16.183','F16.188','F16.19','F16.20','F16.220','F16.221','F16.229','F16.24','F16.250','F16.251','F16.259','F16.280','F16.283','F16.288','F16.29','F18.10','F18.120','F18.121','F18.129','F18.14','F18.150','F18.151','F18.159','F18.17','F18.180','F18.188','F18.19','F18.20','F18.220','F18.221','F18.229','F18.24','F18.250','F18.251','F18.259','F18.27','F18.280','F18.288','F18.29','F19.10','F19.120','F19.121','F19.122','F19.129','F19.14','F19.150','F19.151','F19.159','F19.16','F19.17','F19.180','F19.181','F19.182','F19.188','F19.19','F19.20','F19.220','F19.221','F19.222','F19.229','F19.230','F19.231','F19.232','F19.239','F19.24','F19.250','F19.251','F19.259','F19.26','F19.27','F19.280','F19.281','F19.282','F19.288','F19.29')
    ) t
    ON ptnt_diag.diag_code = t.column_value
   AND ptnt_diag.onset_date >= ADD_MONTHS (TRUNC (SYSDATE, 'MONTH'),-25)
--   AND ptnt_diag.network = 'CBN' AND ptnt_diag.patient_id = 896
),
eligible_ptnt_vsts AS
(
    select --+ materialize
      dt.report_dt, dt.begin_dt, dt.report_dt AS end_dt, vst.admission_dt, LEAD(vst.admission_dt, 1) OVER(PARTITION BY vst.network, vst.patient_id ORDER BY vst.admission_dt ASC) AS followup_vst_dt,
      ROW_NUMBER() OVER (PARTITION BY vst.network, vst.patient_id ORDER BY vst.admission_dt ASC) AS vst_rnum,
      vst.visit_key, vst.network, vst.visit_id, vst.patient_key, vst.patient_id, vst.facility_key, vst.admission_dt_key, /*vst.admission_dt,*/ vst.discharge_dt, vst.visit_number,
      vst.initial_visit_type_id, vst.final_visit_type_id, 
      vst.first_payer_key, vst.last_department_key, 
      diag_ptnts.diag_code, diag_ptnts.diag_coding_scheme, diag_ptnts.onset_date, diag_ptnts.problem_comments
    FROM dt
    JOIN fact_visits vst
      ON vst.admission_dt >= dt.begin_dt 
     AND vst.discharge_dt < dt.report_dt 
    JOIN msrmnt_period_ptnts diag_ptnts
      ON diag_ptnts.network = vst.network 
     AND diag_ptnts.patient_id=vst.patient_id 
     AND diag_ptnts.prblm_onst_dt_rnum = 1
     AND 
     ( 
        (vst.initial_visit_type_id = 2 AND vst.final_visit_type_id = 2) OR 
        (vst.initial_visit_type_id = 3 AND vst.final_visit_type_id = 3) OR 
        (vst.initial_visit_type_id = 4 AND vst.final_visit_type_id = 4) OR
         vst.final_visit_type_id = 1 
      )    
)
SELECT --+ parallel(32)
  t1.report_dt, t1.begin_dt, t1.end_dt, t1.network, fclty.facility_id, fclty.facility_name, t1.patient_id, ptnt.name patient_name, NVL(REGEXP_SUBSTR(t1.visit_number, '^[^-]*'), ptnt.medical_record_number) mrn, 
  t1.visit_id, t1.visit_number, t1.admission_dt, t1.followup_vst_dt,
  ptnt.street_address, ptnt.cell_phone, ptnt.home_phone,
  ptnt.birthdate, ROUND((t1.admission_dt - ptnt.birthdate) / 365) AS age, 
  CASE 
    WHEN UPPER(dim_pyr.payer_name) LIKE '%MEDICAID%' 
    THEN 'Y' 
    ELSE 'N' 
  END AS medicaid_ind,
  dim_pyr.payer_group, dim_pyr.payer_name, --plan_id
  t1.diag_code AS aod_icd_code,
  vst_type.name AS visit_type_name,
  dim_dept.specialty AS clinic,
  dim_dept.department,
  CASE
    WHEN t1.followup_vst_dt - t1.admission_dt <= 14
    THEN 'Y'
    ELSE 'N'
  END numerator_flag
FROM eligible_ptnt_vsts t1
JOIN dim_patients ptnt
  ON t1.network = ptnt.network
 AND t1.patient_id = ptnt.patient_id
 AND ptnt.current_flag = 1
LEFT JOIN dim_hc_facilities fclty
  ON fclty.facility_key = t1.facility_key
LEFT JOIN dim_payers dim_pyr
  ON t1.first_payer_key = dim_pyr.payer_key
LEFT JOIN dim_hc_departments dim_dept
  ON dim_dept.department_key = t1.last_department_key
LEFT JOIN ref_visit_types vst_type
  ON vst_type.visit_type_id = t1.final_visit_type_id
WHERE t1.vst_rnum = 1;