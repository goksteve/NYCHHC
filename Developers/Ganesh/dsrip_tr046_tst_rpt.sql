--drop table dsrip_tr046_tst_rpt;
--Ganesh to fix Empty MRN, Cell Phone
CREATE TABLE dsrip_tr046_tst_rpt
AS
WITH tmp1 AS
(
SELECT * FROM
(
SELECT 
  network,
  patient_id,
  emp_provider_name AS pcp_name, 
  last_pcp_visit_id,
  last_pcp_visit_dt,
  patient_name, 
  mrn, 
  street_address, 
  home_phone, 
  null cell_phone, 
  birthdate AS DOB, 
  ratio AS Asthma_Med_Ratio,
  asthma_other_med_cnt, --AS Count of Other Asthma Medications
  asthma_cntrlr_med_cnt, --AS Count of Asthma Controller Medications 
  NULL AS ip_vst_cnt, --Asthma Referral - No. of IP Visits
  NULL AS ed_vst_cnt --Asthma Referral - No. of ED Visits
FROM dsrip_tr10_asthma_medratio_rpt a
WHERE ratio < 0.5 AND  ASTHMA_OTHER_MED_CNT >= 5 
)),
--UNION ALL 
/* Formatted on 6/4/2018 12:10:45 (QP5 v5.287) */
tmp2 
AS 
(
SELECT
  network,
  patient_id,
  prim_care_provider AS pcp_name,
  name AS PATIENT_NAME, 
  medical_record_number AS MRN,
  street_address, 
  home_phone, 
  cell_number cell_phone,
  birth_date AS DOB, 
  NULL AS Asthma_Med_Ratio,
  NULL asthma_other_med_cnt,
  NULL asthma_cntrlr_med_cnt,
  cnt_ip_visits AS ip_vst_cnt,
  cnt_ed_visits AS ed_vst_cnt
  FROM 
    (
      SELECT
        a.*,
        COUNT(CASE WHEN REGEXP_LIKE (visit_type, 'Inpatient') THEN visit_id END) OVER (PARTITION BY network, patient_id ORDER BY NULL) AS cnt_ip_visits,
        COUNT(CASE WHEN REGEXP_LIKE (visit_type, 'Emergency') THEN visit_id END) OVER (PARTITION BY network, patient_id ORDER BY NULL) AS cnt_ed_visits,
        CASE
          WHEN 
          (   
            COUNT 
            (
              CASE WHEN REGEXP_LIKE (visit_type, 'Inpatient')THEN visit_id END) OVER (PARTITION BY network, patient_id ORDER BY NULL) > 1
              OR 
              COUNT 
              (
                CASE
                  WHEN REGEXP_LIKE (visit_type, 'Emergency')
                  THEN visit_id
                END)
              OVER (PARTITION BY network, patient_id ORDER BY NULL) > 1)
          THEN 1
          ELSE 0
         END AS tr038_flg,
        ROW_NUMBER() OVER (PARTITION BY network, patient_id ORDER BY visit_id DESC) rn
      FROM pt005.tr038_asthma_fnl a
      WHERE REGEXP_LIKE (visit_type, 'Inpatient|Emergency') --group by visit_type
    )
   WHERE tr038_flg > 0 AND rn = 1 
 ) 
 SELECT
--  count(*) , count( distinct network||'~'||patient_id)
-- --'gk',tmp1.* ,'---------','ss',tmp2.*
 COALESCE(tmp1.NETWORK,tmp2.NETWORK) NETWORK
,COALESCE(tmp1.PATIENT_ID,tmp2.PATIENT_ID)PATIENT_ID
,COALESCE(tmp1.PCP_NAME,tmp2.PCP_NAME)PCP_NAME
,tmp1.last_pcp_visit_id
,tmp1.last_pcp_visit_dt
,COALESCE(tmp1.PATIENT_NAME,tmp2.PATIENT_NAME)PATIENT_NAME
,COALESCE(tmp1.MRN,tmp2.MRN)MRN
,COALESCE(tmp1.STREET_ADDRESS,tmp2.STREET_ADDRESS)STREET_ADDRESS
,COALESCE(tmp1.HOME_PHONE,tmp2.HOME_PHONE)HOME_PHONE
,COALESCE(tmp1.CELL_PHONE,tmp2.CELL_PHONE)CELL_PHONE
,COALESCE(tmp1.DOB,tmp2.DOB)DOB
,tmp1.ASTHMA_MED_RATIO
,tmp1.ASTHMA_OTHER_MED_CNT
,tmp1.ASTHMA_CNTRLR_MED_CNT
,tmp2.IP_VST_CNT
,tmp2.ED_VST_CNT
 FROM tmp1 
 FULL OUTER JOIN tmp2 ON tmp1.network = tmp2.network AND tmp1.patient_id = tmp2.patient_id;
 --ORDER BY tmp1.network, tmp2.patient_id;
 