/* Formatted on 5/9/2018 14:57:20 (QP5 v5.287) */
-- GK, 10-May-2018 
WITH comb_data
AS 
(
  SELECT 
    mplus.memberid, qmed.network, qmed.patient_name, fclty.facility_cd, qmed.facility_name, qmed.visit_id, qmed.mrn,
    qmed.dob,
    qmed.age, qmed.home_phone,
    qmed.admission_dt AS pcp_visit_dt,
    NULL visit_provider_name,
    qmed.visit_provider_splty AS visit_provider_speciality
--  FROM cdw.bkp_dsrip_tr43_bh_vsts_rpt2017 qmed /*cdw.dsrip_tr043_bh_visits_rpt_2017*/
  FROM dsrip_bhvisits_042017_03312018 qmed
  LEFT JOIN cdw.dim_hc_facilities fclty
    ON fclty.facility_name = qmed.facility_name
  JOIN dconv.metroplus_assigned_mrn mplus
    ON mplus.mrn = qmed.mrn AND decode (mplus.facility_name, 'LIBE', 'LI', mplus.facility_name) = trim(fclty.facility_cd)
  
  UNION
  
 SELECT 
    mplus.memberid, epic.source, last_name || ',' || first_name, fclty.facility_cd, epic.facility, epic.encounter_id, epic.mrn,
    TO_DATE(epic.birth_date, 'mm/dd/yyyy') as dob,
    epic.age, epic.home_phone,
    to_date(substr(epic.date_of_visit,1,11), 'YYYY-MM-DD') AS date_of_visit,
    epic.visit_provider_type, epic.visit_provider_speciality
  FROM epic_clarity.dsrip_tr043_epic_bh_visits epic
  JOIN dconv.metroplus_assigned_mrn mplus
    ON mplus.mrn = epic.mrn AND decode (mplus.facility_name, 'LIBE', 'LI', mplus.facility_name) =
            CASE 
          WHEN SUBSTR(epic.facility,1,2) = 'EL' THEN 'EL'
          WHEN SUBSTR(epic.facility,1,2) = 'QU' THEN 'QU'
          ELSE 'CI'
        END
  LEFT JOIN cdw.dim_hc_facilities fclty
    ON fclty.facility_cd = 
        CASE 
          WHEN SUBSTR(epic.facility,1,2) = 'EL' THEN 'EL'
          WHEN SUBSTR(epic.facility,1,2) = 'QU' THEN 'QU'
          ELSE 'CI'
        END
  WHERE to_date(substr(date_of_visit,1,10), 'yyyy-mm-dd') >= date '2017-04-01' AND to_date(substr(date_of_visit,1,10), 'yyyy-mm-dd') < date '2018-05-01'                
),
dat
AS 
(
  SELECT --+ parallel(32)

      DISTINCT a.memberkey,
      a.cin,
      a.memberlastname,
      a.memberfirstname,
      a.dob as mplus_dateofbirth,
      b.dob as hhc_dateofbirth,
      a.dischargingfacility,
      a.mrn,
      a.dischargedate,
      to_char(b.pcp_visit_dt, 'MM/DD/YYYY') AS pcp_visit_dt, 
      b.facility_name bh_vst_facility, 
      b.network,
      b.visit_provider_speciality,
      CASE
        WHEN b.pcp_visit_dt - to_date(a.dischargedate,'MM/DD/YYYY') BETWEEN 0 AND 30
        THEN 'Y'
        ELSE 'N'
      END AS flag
    FROM FUHGAPS_MPLUS_APR2017_MAY2018 a
    JOIN comb_data b on a.cin = b.memberid and a.mrn = b.mrn
)
SELECT  --distinct membernum
  * FROM dat --WHERE flag = 'Y'
--where upper(substr(membername, 1, instr(membername,','))) != upper(substr(patient_name, 1, instr(patient_name,',')))
ORDER BY 1;


SELECT 
    mplus.memberid, qmed.network, qmed.patient_name, fclty.facility_cd, qmed.facility_name, qmed.visit_id, qmed.mrn,
    qmed.dob,
    qmed.age, qmed.home_phone,
    qmed.admission_dt AS pcp_visit_dt,
    NULL visit_provider_name,
    qmed.visit_provider_splty AS visit_provider_speciality
--  FROM cdw.bkp_dsrip_tr43_bh_vsts_rpt2017 qmed /*cdw.dsrip_tr043_bh_visits_rpt_2017*/
  FROM cdw.dsrip_tr043_bh_visits_report qmed
  LEFT JOIN cdw.dim_hc_facilities fclty
    ON fclty.facility_name = qmed.facility_name
  JOIN dconv.metroplus_assigned_mrn mplus
    ON mplus.mrn = qmed.mrn AND decode (mplus.facility_name, 'LIBE', 'LI', mplus.facility_name) = trim(fclty.facility_cd)
    where mplus.memberid='QU67771A'
    
    
    
    select to_date(dischargedate,'mm/dd/yyyy') from FUHGAPS_MPLUS_APR2017_MAY2018 order by 1 desc
    FUHGAPS_EMBLEMHEALTH