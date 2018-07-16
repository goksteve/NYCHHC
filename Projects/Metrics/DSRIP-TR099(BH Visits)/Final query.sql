WITH mplus_data
 -- 28-Jun-2018, GK: Mplus matching patients with HHC_BH_visits(qmed & epic) using patient_name & Date_of_birth 
AS 
(
  SELECT 
    'MPLUS' plan_name,
    t1.*, 
--    qmed.MSRMNT_PERIOD AS hhc_MSRMNT_PERIOD,		
--    qmed.NETWORK	AS hhc_network,
    'QMED' AS HHC_SOURCE,
    qmed.PATIENT_ID	AS hhc_patient_id,
    qmed.PATIENT_NAME	AS hhc_patient_name,
    qmed.VISIT_ID	AS hhc_visit_id,
--    qmed.VISIT_NUMBER	AS hhc_visit_number, 
    qmed.MRN AS hhc_ptnt_mrn,	
    qmed.STREET_ADDRESS AS hhc_ptnt_street_address,	
    qmed.APT_SUITE AS hhc_ptnt_apt_suite,
    qmed.CITY AS hhc_ptnt_city,	
    qmed.STATE	AS hcc_ptnt_state,
--    qmed.COUNTRY	AS hhc_ptnt_country,
    qmed.ZIP_CODE	AS hhc_ptnt_zip_code,
    qmed.HOME_PHONE	AS hhc_ptnt_home_phone,	
    qmed.CELL_PHONE	AS 	hhc_ptnt_cell_phone,
    qmed.PCP_GENERAL_MED	AS hhc_ptnt_assigned_pcp,
    qmed.VISIT_PROVIDER_NAME AS hhc_vst_prvdr_name,
    qmed.VISIT_PROVIDER_SPLTY AS hhc_VISIT_PROVIDER_SPLTY,
    qmed.FACILITY_NAME AS hhc_vst_FACILITY_NAME,
    qmed.ADMISSION_DT AS hhc_vst_ADMISSION_DT,
    qmed.DISCHARGE_DT AS hhc_vst_DISCHARGE_DT
  FROM dsrip_bhvisits_042017_03312018 qmed
  JOIN FUHGAPS_MPLUS_APR2017_MAY2018 t1
    ON TO_DATE(t1.dob,'MM/DD/YYYY') = qmed.dob
   AND INSTR(UPPER(qmed.patient_name),t1.memberlastname,1) > 0
--  ORDER BY t1.patient_name, t1.dob

  UNION

  SELECT 
    'MPLUS' plan_name,
    t1.*,
    epic.source as HHC_SOURCE,
    cast(epic.empi as number) as empi,
    epic.last_name||', '||epic.first_name	AS hhc_patient_name,
    epic.encounter_id	AS hhc_visit_id,
--    qmed.VISIT_NUMBER	AS hhc_visit_number, 
    epic.MRN AS hhc_ptnt_mrn,	
    epic.STREET_ADRESS AS hhc_ptnt_street_address,	
    epic.APT_SUITE AS hhc_ptnt_apt_suite,
    epic.CITY AS hhc_ptnt_city,	
    epic.STATE	AS hcc_ptnt_state,
--    qmed.COUNTRY	AS hhc_ptnt_country,
    epic.ZIP_CODE	AS hhc_ptnt_zip_code,
    epic.HOME_PHONE	AS hhc_ptnt_home_phone,	
    epic.mobile_PHONE	AS 	hhc_ptnt_cell_phone,
    'N/A'	AS hhc_ptnt_assigned_pcp,
    epic.VISIT_PROVIDER_NAME AS hhc_vst_prvdr_name,
    epic.VISIT_PROVIDER_SPECIALITY AS hhc_VISIT_PROVIDER_SPLTY,
    epic.FACILITY AS hhc_vst_FACILITY_NAME,
    TO_DATE(SUBSTR(date_of_visit,1,10), 'YYYY-MM-DD') AS hhc_vst_ADMISSION_DT,
    TO_DATE(SUBSTR(checkout_time,1,10), 'YYYY-MM-DD') AS hhc_vst_DISCHARGE_DT
  FROM epic_clarity.dsrip_tr043_epic_bh_visits epic
  JOIN  FUHGAPS_MPLUS_APR2017_MAY2018 t1
    ON TO_DATE(t1.dob,'MM/DD/YYYY') = TO_DATE(epic.birth_date,'MM/DD/YYYY')
   AND INSTR(UPPER(epic.last_name),t1.memberlastname,1) > 0
  WHERE TO_DATE(SUBSTR(date_of_visit,1,10), 'YYYY-MM-DD') >= DATE '2017-04-01' AND TO_DATE(SUBSTR(date_of_visit,1,10), 'YYYY-MM-DD') < DATE '2018-05-01'                
), 
fuh_mplus_apr17_may18 AS
(
SELECT
--  mplus_data.*,
  distinct
  a.plan_name,
  a.memberkey,
  a.cin,
  a.memberlastname,
  a.memberfirstname,
  a.dob as mplus_dateofbirth,
  a.dischargingfacility,
  a.mrn,
  a.hhc_ptnt_mrn,
  a.dischargedate,
  a.hhc_vst_admission_dt AS pcp_visit_dt, 
  a.hhc_vst_facility_name AS bh_vst_facility, 
  a.hhc_source,
  a.hhc_visit_provider_splty visit_provider_speciality,
  CASE
    WHEN a.hhc_vst_admission_dt - to_date(a.dischargedate,'MM/DD/YYYY') BETWEEN 0 AND 30
    THEN 'Y'
    ELSE 'N'
  END AS flag
  FROM mplus_data a
)
SELECT 
  a.*
FROM 
(
  SELECT 
    t.*, ROW_NUMBER () OVER (PARTITION BY cin, dischargedate ORDER BY pcp_visit_dt) AS rn FROM fuh_mplus_apr17_may18 t
  WHERE                                               --cin='630149642'               --and
  pcp_visit_dt > TO_DATE (dischargedate, 'MM/DD/YYYY')
)a
WHERE     
  CASE
    WHEN a.pcp_visit_dt - TO_DATE (a.dischargedate, 'MM/DD/YYYY') BETWEEN 1 AND 30
    THEN 'Y'
    ELSE 'N'
  END = 'Y'
AND rn = 1