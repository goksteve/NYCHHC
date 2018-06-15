--DROP TABLE dsrip_tr046_tst_rpt_v2;

CREATE TABLE dsrip_tr046_tst_rpt_v2
NOLOGGING
PARALLEL 32
AS
--young and adult asthma dataset
WITH xx_tmp2 
AS
(
  SELECT --+ parallel(32) 
    a.*,--,fvsts.visit_id pcp_visit_id,fvsts.admission_dt pcp_visit_dt,
    COUNT(CASE WHEN REGEXP_LIKE (a.visit_type, 'Inpatient') THEN a.visit_id END) OVER (PARTITION BY a.network, a.patient_id ORDER BY NULL) AS cnt_ip_visits,
    COUNT(CASE WHEN REGEXP_LIKE (a.visit_type, 'Emergency') THEN a.visit_id END) OVER (PARTITION BY a.network, a.patient_id ORDER BY NULL) AS cnt_ed_visits,
    CASE
      WHEN 
      (   
        COUNT( CASE WHEN REGEXP_LIKE (a.visit_type, 'Inpatient')THEN a.visit_id END) OVER (PARTITION BY a.network, a.patient_id ORDER BY NULL) > 1
        OR 
        COUNT( CASE WHEN REGEXP_LIKE (a.visit_type, 'Emergency') THEN a.visit_id END) OVER (PARTITION BY a.network, a.patient_id ORDER BY NULL) > 1
      )
      THEN 1
      ELSE 0
    END AS tr038_flg,
    ROW_NUMBER() OVER (PARTITION BY a.network, a.patient_id ORDER BY a.visit_id DESC) rn
  FROM 
  (
    SELECT 
      network, visit_id ,patient_id, prim_care_provider,name,medical_record_number,street_address,
      home_phone,cell_number,birth_date,visit_type,facility_name,city,mailing_code, payer_name 
    FROM pt005.tr038_asthma_fnl
    UNION ALL
    SELECT 
      network, visit_id, patient_id, prim_care_provider,name,medical_record_number,street_address,
      home_phone,cell_number,birth_date,visit_type,facility_name,city,mailing_code, payer_name
    from pt005.tr038_adlt_asthma_fnl 
    ) a
),
xx_tmp2_pcp_vsts AS
(
  SELECT
    fvsts.network, fvsts.patient_id, fvsts.visit_id, fvsts.admission_dt, ref_vt.name AS pcp_visit_type, NVL(vst.addl_resp_emp_provider_id, attending_emp_provider_id) pcp_provider_id,
    row_number() over (partition by fvsts.network, fvsts.patient_id order by admission_dt desc) fvsts_rnum 
  FROM xx_tmp2 a
  JOIN cdw.fact_visits fvsts
    ON fvsts.network = a.network  and fvsts.patient_id = a.patient_id
  JOIN cdw.visit vst
    ON vst.network = fvsts.network AND vst.visit_id = fvsts.visit_id
  JOIN dim_hc_departments dept
    ON dept.department_key = fvsts.first_department_key AND dept.service_type = 'PCP'
  LEFT JOIN cdw.ref_visit_types ref_vt
    ON ref_vt.visit_type_id = fvsts.initial_visit_type_id
),
tmp2
AS
(
SELECT  --+ parallel(32)
--xx_tmp1.*,vst.visit_id lst_pcp_visit_id, vst.admission_dt lst_pcp_visit_dt
--  SELECT
    xx.network,
    xx.patient_id,
    xx.prim_care_provider AS pcp_name,
    xx.name AS PATIENT_NAME, 
    xx.medical_record_number AS MRN,
    xx.street_address, 
    xx.home_phone, 
    xx.cell_number cell_phone,
    xx.birth_date AS DOB, 
    NULL AS Asthma_Med_Ratio,
    NULL asthma_other_med_cnt,
    NULL asthma_cntrlr_med_cnt,
    xx.cnt_ip_visits AS ip_vst_cnt,
    xx.cnt_ed_visits AS ed_vst_cnt,
    xx.facility_name,
    xx.city,
    xx.mailing_code,
    xx.payer_name,
    vst.visit_id last_pcp_visit_id,
    vst.admission_dt AS last_pcp_visit_dt,
    vst.pcp_visit_type AS last_pcp_visit_type,
    prvdr.provider_name as last_pcp_vst_provider
FROM xx_tmp2 xx
LEFT JOIN xx_tmp2_pcp_vsts vst
  ON xx.network = vst.network 
 AND xx.patient_id = vst.patient_id
LEFT JOIN cdw.dim_providers prvdr
  ON prvdr.network = vst.network AND prvdr.provider_id = vst.pcp_provider_id AND prvdr.current_flag = 1
WHERE rn = 1 and fvsts_rnum=1 and tr038_flg > 0
),

--asthma medication ratio
xx_tmp1 AS
(
  SELECT -- parallel(32)
    a.network,
    a.patient_id,
    a.emp_provider_name AS pcp_name, 
    a.patient_name, 
    a.mrn, 
    a.street_address, 
    a.home_phone, 
    cphone.cell_number AS cell_phone, 
    a.birthdate AS DOB, 
    a.ratio AS Asthma_Med_Ratio,
    a.asthma_other_med_cnt, --AS Count of Other Asthma Medications
    a.asthma_cntrlr_med_cnt, --AS Count of Asthma Controller Medications 
    NULL AS ip_vst_cnt, --Asthma Referral - No. of IP Visits
    NULL AS ed_vst_cnt, --Asthma Referral - No. of ED Visits
    a.facility_name,
    a.city,
    a.mailing_code,
    a.payer_name
--    a.LAST_PCP_VISIT_DT  
  FROM dsrip_tr10_asthma_medratio_rpt a
  LEFT JOIN
  (
    SELECT 
      a.network, a.patient_id, LISTAGG (VALUE, ' | ') WITHIN GROUP (ORDER BY item_number) OVER (PARTITION BY a.network, a.patient_id) AS cell_number
    FROM cdw.patient_generic_data a
    JOIN cdw.patient_generic_field b
      ON a.patient_generic_field_id = b.patient_generic_field_id AND a.network = b.network
     AND REGEXP_LIKE (UPPER (b.name), 'CELL')
  ) cphone
  ON a.network = cphone.network AND a.patient_id = cphone.patient_id

  WHERE ASTHMA_OTHER_MED_CNT >= 5 
),
xx_tmp1_pcp_vsts AS
(
  SELECT
    fvsts.network, fvsts.patient_id, fvsts.visit_id, fvsts.admission_dt, ref_vt.name AS pcp_visit_type, NVL(vst.addl_resp_emp_provider_id, attending_emp_provider_id) pcp_provider_id,
    row_number() over (partition by fvsts.network, fvsts.patient_id order by admission_dt desc) fvsts_rnum 
  FROM xx_tmp1 a
  JOIN cdw.fact_visits fvsts
    ON fvsts.network = a.network  and fvsts.patient_id = a.patient_id
  JOIN cdw.visit vst
    ON vst.network = fvsts.network AND vst.visit_id = fvsts.visit_id
  JOIN dim_hc_departments dept
    ON dept.department_key = fvsts.first_department_key AND dept.service_type = 'PCP'
  LEFT JOIN cdw.ref_visit_types ref_vt
    ON ref_vt.visit_type_id = fvsts.initial_visit_type_id

),
tmp1 AS
(
  SELECT -- parallel(32)
    a.network,
    a.patient_id,
    a.pcp_name, 
    a.patient_name, 
    a.mrn, 
    a.street_address, 
    a.home_phone, 
    a.cell_phone, 
    a.DOB, 
    a.Asthma_Med_Ratio,
    a.asthma_other_med_cnt, --AS Count of Other Asthma Medications
    a.asthma_cntrlr_med_cnt, --AS Count of Asthma Controller Medications 
    NULL AS ip_vst_cnt, --Asthma Referral - No. of IP Visits
    NULL AS ed_vst_cnt, --Asthma Referral - No. of ED Visits
    a.facility_name,
    a.city,
    a.mailing_code,
    a.payer_name,
    b.visit_id last_pcp_visit_id,
    b.admission_dt AS last_pcp_visit_dt,
    b.pcp_visit_type AS last_pcp_visit_type,
    prvdr.provider_name as last_pcp_vst_provider    
  FROM xx_tmp1 a
  LEFT JOIN xx_tmp1_pcp_vsts b
  ON a.network = b.network AND a.patient_id = b.patient_id
  AND b.fvsts_rnum = 1
  LEFT JOIN cdw.dim_providers prvdr
  ON prvdr.network = b.network AND prvdr.provider_id = b.pcp_provider_id AND prvdr.current_flag = 1
)    
SELECT -- parallel(32)
  COALESCE(tmp1.network, tmp2.network) AS network,
  COALESCE(tmp1.patient_id, tmp2.patient_id) AS patient_id,
  COALESCE(tmp1.pcp_name, tmp2.pcp_name) AS pcp_name,
  COALESCE(tmp1.patient_name, tmp2.patient_name) AS patient_name,
  COALESCE(tmp1.mrn, tmp2.mrn) AS mrn,
  COALESCE(tmp1.street_address, tmp2.street_address) AS street_address,
  COALESCE(tmp1.home_phone, tmp2.home_phone) AS home_phone,
  COALESCE(tmp1.cell_phone, tmp2.cell_phone) AS cell_phone,
  COALESCE(tmp1.dob, tmp2.dob) AS dob,
  COALESCE(tmp1.facility_name, tmp2.facility_name) AS facility_name,
  COALESCE(tmp1.city, tmp2.city) AS city,
  COALESCE(tmp1.mailing_code, tmp2.mailing_code) AS mailing_code,
  COALESCE(tmp1.payer_name, tmp2.payer_name) AS payer_name,
  COALESCE(tmp1.last_pcp_visit_id, tmp2.last_pcp_visit_id) AS last_pcp_visit_id,
  COALESCE(tmp1.last_pcp_visit_dt, tmp2.last_pcp_visit_dt) AS last_pcp_visit_dt,
  COALESCE(tmp1.last_pcp_visit_type, tmp2.last_pcp_visit_type) AS last_pcp_visit_type,
  COALESCE(tmp1.last_pcp_vst_provider, tmp2.last_pcp_vst_provider) AS last_pcp_vst_provider,
  tmp1.asthma_med_ratio,
  tmp1.asthma_other_med_cnt,
  tmp1.asthma_cntrlr_med_cnt,
  tmp2.ip_vst_cnt,
  tmp2.ed_vst_cnt
FROM tmp1
FULL OUTER JOIN tmp2
  ON tmp1.network = tmp2.network
 AND tmp1.patient_id = tmp2.patient_id;
 
 
--######################################################################################################################################################################### 
--  Verification SQL's 
--######################################################################################################################################################################### 
 /*
 
 
SELECT --+ parallel(32)
  count(*) , count( distinct network||'~'||patient_id)
  from dsrip_tr046_tst_rpt_v3; 
 
 select * from dsrip_tr046_tst_rpt_v3;

Select network||'~'||patient_id
--count( distinct network||'~'||patient_id)
from tst_gk_tr045_rpt06122018_3
group by (network||'~'||patient_id)
having count( network||'~'||patient_id) > 1
;

*/