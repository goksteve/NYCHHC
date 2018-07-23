select distinct cin from DSRIP_FUH_MPLUS_APR17_MAY18_V1;

--drop table DSRIP_FUH_MPLUS_APR17_MAY18_V1;
CREATE TABLE DSRIP_FUH_MPLUS_APR17_MAY18_V1
AS
WITH comb_data
AS 
 -- 28-Jun-2018, GK: Mplus matching patients with HHC_BH_visits(qmed & epic) using ETL file's facility MRN(dconv.metroplus_assigned_mrn)
(
  SELECT 
    mplus.memberid, qmed.network, qmed.patient_name, fclty.facility_cd, qmed.facility_name, qmed.visit_id, qmed.mrn,
    qmed.dob,
    qmed.age, qmed.home_phone,
    qmed.admission_dt AS pcp_visit_dt,
    NULL visit_provider_name,
    qmed.visit_provider_splty AS visit_provider_speciality
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
    distinct 'MPLUS' plan_name,
    a.memberkey,
    a.cin,
    a.memberlastname,
    a.memberfirstname,
    a.dob as mplus_dateofbirth,
    a.dischargingfacility,
    a.mrn,
    a.dischargedate,
    b.pcp_visit_dt,    
    b.facility_name bh_vst_facility, 
    CASE 
      WHEN facility_name in 
      (
        'ELMHURST HOSPITAL CENTER',
        'QU JAIMACA HIGH SCHOOL (MENTAL HEALTH)',
        'EL I.S. 145 SBHC',
        'QUEENS HOSPITAL CENTER',
        'QU PS 160 (MENTAL HEALTH)',
        'CONEY ISLAND HOSPITAL CENTER',
        'QU PS 154 (MENTAL HEALTH)'
      )
      THEN 'EPIC'
      ELSE 'QMED'
    END AS source,  
    b.visit_provider_speciality,
    CASE
      WHEN b.pcp_visit_dt - to_date(a.dischargedate,'MM/DD/YYYY') BETWEEN 0 AND 30
      THEN 'Y'
      ELSE 'N'
    END AS flag
  FROM FUHGAPS_MPLUS_APR2017_MAY2018 a
  JOIN comb_data b on a.cin = b.memberid and a.mrn = b.mrn
)
SELECT * FROM dat;