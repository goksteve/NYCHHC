-- ** Title:	Compass 2.0 DSRIP Tiles SQL  Queries**--
-- ** Description:	SQL Queries for 5 DSRIP Reports TR019 Prevention Quality Indicator 90 (PQI 90), TR016 Diabetes Screening, TR017 Diabetes Monitoring, TR001 30 and 7 Day follow-up post psychiatric hospitalization
-- **               and TR002_023 Comprehensive Diabetes Care.   
-- ** Tags:			
-- ** Updates:		Initial Create-05/14/2018
-- **             Changes in SQL, to 
--drop view vw_pqi7
CREATE OR REPLACE VIEW /*vw_pqi7*/ vw_dsrip_pqi90_7_compass
AS
  -- 18-May-2018, GK: Added extra date condition to result only the latest month data
  -- 16-May-2018, GK: TR019 Prevention Quality Indicator 90 (PQI 90) Hypertension discharges
WITH dsrip_pqi90_78
AS
(
  SELECT 
    TO_DATE(TO_CHAR(period_start_dt, 'MON-YYYY'), 'MON-YYYY')  AS dt,
    a.facility_name,
    a.denominator AS total_ip_discharges,
    a.numerator_1 AS htn_disch_diag,
    MAX(period_start_dt) OVER (PARTITION BY network, report_cd ORDER BY NULL) AS max_dt
  FROM pt005.dsrip_report_results a
  WHERE report_cd = 'PQI90-78' 
)
SELECT 
  'PQI - 7(Hypertension)' box_header,
  'INPATIENT_DISCHARGES' unique_tag,
----------------------------
  'Total number of inpatients discharged' AS text_1,
  Total_IP_discharges AS sub_text_1,
  NULL AS sub_text_description_1,
  prev_Total_IP_discharges AS prev_sub_text_1,
  ROUND ( (Total_IP_discharges - prev_Total_IP_discharges) / Total_IP_discharges * 100, 1) || '%' AS value1,
  'Prev month and current month variance for Total IP Discharges' AS value_desc_1,
  CASE
    WHEN ROUND ( (Total_IP_discharges - prev_Total_IP_discharges) / Total_IP_discharges * 100, 0) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_1,
----------------------------------------------
  'Total number of patients discharged with a diagnosis of hypertension.' AS text_2,
  Htn_disch_diag AS sub_text_2,
  NULL AS sub_text_description_2,
  prev_Htn_disch_diag AS prev_sub_text_2,
  ROUND ( (Htn_disch_diag - prev_Htn_disch_diag) / Htn_disch_diag * 100, 1) || '%' AS value2,
  'Prev month and current month variance for Total number of patients discharged with a diagnosis of hypertension.' AS value_desc_2,
  CASE
    WHEN ROUND ( (Htn_disch_diag - prev_Htn_disch_diag) / Htn_disch_diag * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_2,
-----------------------------------------------
  NULL AS text_3,
  NULL AS sub_text_3,
  NULL AS sub_text_description_3,
  NULL AS prev_sub_text_3,
  NULL AS value3,
  NULL AS value_desc_3,
  NULL AS performance_indicator_3,
-----------------------------------------------
  NULL AS text_4,
  NULL AS sub_text_4,
  NULL AS sub_text_description_4,
  NULL AS prev_sub_text_4,
  NULL AS value4,
  NULL AS value_desc_4,
  NULL AS performance_indicator_4,
----------------------------------------------
  NULL network_name,
  facility_code,
  facility_name,
  'M' time_period_indicator,
  EXTRACT (YEAR FROM dt) yr,
  NULL quarter,
  EXTRACT (MONTH FROM dt) mnth,
  TO_CHAR (dt, 'Month') month_text,
  NULL AS date_value,
  SYSDATE create_dt,
--  'kollurug' create_by
  sys_context('USERENV', 'OS_USER') create_by,
  null cognos_dashboard_url
----------------------------------------------
FROM
(
  SELECT 
    dt,
    a.facility_name,
    CASE
      WHEN a.facility_name = 'ALL facilities'
      THEN 'HHC'

      ELSE f.facility_code
    END AS facility_code,
--------------------------
    Total_IP_discharges,
    LEAD (Total_IP_discharges, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_Total_IP_discharges,
--------------------------
    Htn_disch_diag,
    LEAD (Htn_disch_diag, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_Htn_disch_diag,
--------------------------    
    ROW_NUMBER () OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS rn
  FROM dsrip_pqi90_78 a
  LEFT JOIN pt008.facility_mapping f
    ON a.facility_name = f.facility_name
  WHERE 1 = 1
  AND Total_IP_discharges <> 0
  AND Htn_disch_diag <> 0
)
WHERE rn =1
-- where condition to include latest month data for all facilities
AND EXTRACT(MONTH FROM dt) = 
(
  SELECT  
    EXTRACT (MONTH FROM MAX(period_start_dt)) 
  FROM pt005.dsrip_report_results a
  WHERE report_cd = 'PQI90-78'
) ;

INSERT INTO compass_metrics_beta SELECT * FROM vw_dsrip_pqi90_7_compass;
COMMIT;
--delete from compass_metrics_beta WHERE unique_tag = 'INPATIENT_DISCHARGES';
SELECT * FROM compass_metrics_beta WHERE unique_tag = 'INPATIENT_DISCHARGES';

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- drop view vw_dsrip_tr016_AntipyschRx
CREATE OR REPLACE VIEW /*vw_dsrip_tr016_AntipyschRx*/ vw_dsrip_tr016_compass
AS
  -- 18-May-2018, GK: Added extra date condition to result only the latest month data
  -- 15-May-2018, GK: TR016 Diabetes Screening for People with Schizophrenia or Bipolar Disease who are Using Antipsychotic Medication  
WITH dsrip_tr016
AS
(
  SELECT 
    to_date(to_char(period_start_dt, 'MON-YYYY'), 'MON-YYYY')  AS dt,
    a.facility_name,
    a.denominator AS Pat18_64_Wth_AntipyschRx,
    A.NUMERATOR_1 AS pat_diagnosed_with_diab
  FROM pt005.dsrip_report_results a
  WHERE report_cd = 'DSRIP-TR016' and a.facility_name != 'Health and Hospitals Corporation' and a.facility_name !='Unknown' 
)
SELECT 
  'BH - Diabetes Screening' box_header,
  'DSRIP_DIAB_ANTIPSYCH_MED' unique_tag,
----------------------------
  'Total number of patients in the age group 18 to 64 years, on Anti-Psychotic medications.' AS text_1,
  Pat18_64_Wth_AntipyschRx AS sub_text_1,
  NULL AS sub_text_description_1,
  prev_Pat18_64_Wth_AntipyschRx AS prev_sub_text_1,
  ROUND ( (Pat18_64_Wth_AntipyschRx - prev_Pat18_64_Wth_AntipyschRx) / Pat18_64_Wth_AntipyschRx * 100, 1) || '%' AS value1,
  'Prev month and current month variance for Total no. of Patients on Anti-Psych med' AS value_desc_1,
  CASE
    WHEN ROUND ( (Pat18_64_Wth_AntipyschRx - prev_Pat18_64_Wth_AntipyschRx) / Pat18_64_Wth_AntipyschRx * 100, 0) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_1,
----------------------------------------------
  'Number of people who had a glucose or HbA1c test' AS text_2,
  pat_diagnosed_with_diab AS sub_text_2,
  NULL AS sub_text_description_2,
  prev_pat_diagnosed_with_diab AS prev_sub_text_2,
  ROUND ( (pat_diagnosed_with_diab - prev_pat_diagnosed_with_diab) / pat_diagnosed_with_diab * 100, 1) || '%' AS value2,
  'Prev month and current month variance for Total Number of people who had a glucose or HbA1c test' AS value_desc_2,
  CASE
    WHEN ROUND ( (pat_diagnosed_with_diab - prev_pat_diagnosed_with_diab) / pat_diagnosed_with_diab * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_2,
-----------------------------------------------
  NULL AS text_3,
  NULL AS sub_text_3,
  NULL AS sub_text_description_3,
  NULL AS prev_sub_text_3,
  NULL AS value3,
  NULL AS value_desc_3,
  NULL AS performance_indicator_3,
-----------------------------------------------
  NULL AS text_4,
  NULL AS sub_text_4,
  NULL AS sub_text_description_4,
  NULL AS prev_sub_text_4,
  NULL AS value4,
  NULL AS value_desc_4,
  NULL AS performance_indicator_4,
----------------------------------------------
  NULL network_name,
  facility_code,
  facility_name,
  'M' time_period_indicator,
  EXTRACT (YEAR FROM dt) yr,
  NULL quarter,
  EXTRACT (MONTH FROM dt) mnth,
  TO_CHAR (dt, 'Month') month_text,
  NULL AS date_value,
  SYSDATE create_dt,
--  'kollurug' create_by
  sys_context('USERENV', 'OS_USER') create_by,
  'http://eimtest.nychhc.org/ibmcognos/cgi-bin/cognos.cgi?b_action=cognosViewer||chr(38)||ui.action=run||chr(38)||ui.object=%2fcontent%2ffolder%5b%40name%3d%27Reports%27%5d%2ffolder%5b%40name%3d%27DSRIP%20Metrics%27%5d%2ffolder%5b%40name%3d%27DSRIP%20Metrics%27%5d%2freport%5b%40name%3d%27Diabetes%20Screening%27%5d||chr(38)||ui.name=Diabetes%20Screening||chr(38)||run.outputFormat=||chr(38)||run.prompt=true||chr(38)||ui.backURL=%2fibmcognos%2fcgi-bin%2fcognos.cgi%3fb_action%3dxts.run%26m%3dportal%2fcc.xts%26m_folder%3di452592FA64524A048A981D6DD3BABBD4' cognos_dashboard_url

----------------------------------------------
FROM
(
  SELECT 
    dt,
    a.facility_name,
    CASE
      WHEN a.facility_name = 'ALL facilities'
      THEN 'HHC'
      ELSE f.facility_code
    END AS facility_code,
--------------------------
    Pat18_64_Wth_AntipyschRx,
    LEAD (Pat18_64_Wth_AntipyschRx, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_Pat18_64_Wth_AntipyschRx,
--------------------------
    pat_diagnosed_with_diab,
    LEAD (pat_diagnosed_with_diab, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_pat_diagnosed_with_diab,
--------------------------
    ROW_NUMBER () OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS rn
  FROM dsrip_tr016 a
  LEFT JOIN pt008.facility_mapping f
    ON a.facility_name = f.facility_name
  WHERE 1 = 1
  AND Pat18_64_Wth_AntipyschRx <> 0
  AND pat_diagnosed_with_diab <> 0
)
WHERE rn =1 
AND EXTRACT (MONTH FROM dt) = 
(
  SELECT  
    EXTRACT (MONTH FROM MAX(period_start_dt)) 
  FROM pt005.dsrip_report_results a
  WHERE report_cd = 'DSRIP-TR016' and a.facility_name != 'Health and Hospitals Corporation' and a.facility_name !='Unknown' 
);

INSERT INTO compass_metrics_beta SELECT * FROM vw_dsrip_tr016_compass;
COMMIT;
--delete from compass_metrics_beta WHERE unique_tag = 'DSRIP_DIAB_ANTIPSYCH_MED';
SELECT * FROM compass_metrics_beta WHERE unique_tag = 'DSRIP_DIAB_ANTIPSYCH_MED';

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--drop view vw_dsrip_diab_schizo;
CREATE OR REPLACE VIEW /*vw_dsrip_diab_schizo*/ vw_dsrip_tr017_compass
AS
  -- 18-May-2018, GK: Added extra date condition to result only the latest month data
  -- 14-May-2018, GK: TR017 Diabetes Monitoring for People with Diabetes and Schizophrenia
WITH dsrip_diab_schizo
AS 
(  
  SELECT 
    to_date(TO_CHAR ("Report Month", 'MON-YYYY'), 'MON-YYYY')  AS dt,
    "Facility Name" AS facility_name,
    "# Patients" AS Pats_with_Diab_Schizophrenia,
    "# Patient with Both Results" pats_with_both_results
  FROM pt005.v_tr017_diab_mon_sum_cdw_all
)
SELECT 
  'BH - Diabetes Monitoring' box_header,
  'DSRIP_DIAB_SCHRIZOPHRENIA' unique_tag,
----------------------------
  'Total number of people, ages 18 to 64 years with Schizophrenia and Diabetes, based on diagnosis' AS text_1,
  Pats_with_Diab_Schizophrenia AS sub_text_1,
  NULL AS sub_text_description_1,
  prev_Pats_with_Diab_Schiz AS prev_sub_text_1,
  ROUND ( (Pats_with_Diab_Schizophrenia - prev_Pats_with_Diab_Schiz) / Pats_with_Diab_Schizophrenia * 100, 1) || '%' AS value1,
  'Prev month and current month variance for Total no. of Patients with Diabetes and Schizophrenia' AS value_desc_1,
  CASE
    WHEN ROUND ( (Pats_with_Diab_Schizophrenia - prev_Pats_with_Diab_Schiz) / Pats_with_Diab_Schizophrenia * 100, 0) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_1,
----------------------------------------------
  'Number of people who had both an LDL-C test and an HbA1c test during the measurement year' AS text_2,
  pats_with_both_results AS sub_text_2,
  NULL AS sub_text_description_2,
  prev_pats_with_both_results AS prev_sub_text_2,
  ROUND ( (pats_with_both_results - prev_pats_with_both_results) / pats_with_both_results * 100, 1) || '%' AS value2,
  'Prev month and current month variance for Total Number of people who had both an LDL-C test and an HbA1c test during the measurement year' AS value_desc_2,
  CASE
    WHEN ROUND ( (pats_with_both_results - prev_pats_with_both_results) / pats_with_both_results * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_2,
-----------------------------------------------
  NULL AS text_3,
  NULL AS sub_text_3,
  NULL AS sub_text_description_3,
  NULL AS prev_sub_text_3,
  NULL AS value3,
  NULL AS value_desc_3,
  NULL AS performance_indicator_3,
-----------------------------------------------
  NULL AS text_4,
  NULL AS sub_text_4,
  NULL AS sub_text_description_4,
  NULL AS prev_sub_text_4,
  NULL AS value4,
  NULL AS value_desc_4,
  NULL AS performance_indicator_4,
----------------------------------------------
  NULL network_name,
  facility_code,
  facility_name,
  'M' time_period_indicator,
  EXTRACT (YEAR FROM dt) yr,
  NULL quarter,
  EXTRACT (MONTH FROM dt) mnth,
  TO_CHAR (dt, 'Month') month_text,
  NULL AS date_value,
  SYSDATE create_dt,
--  'kollurug' create_by
  sys_context('USERENV', 'OS_USER') create_by,
  'http://eimtest.nychhc.org/ibmcognos/cgi-bin/cognos.cgi?b_action=cognosViewer||chr(38)||ui.action=run||chr(38)||ui.object=%2fcontent%2ffolder%5b%40name%3d%27Reports%27%5d%2ffolder%5b%40name%3d%27DSRIP%20Metrics%27%5d%2ffolder%5b%40name%3d%27DSRIP%20Metrics%27%5d%2freport%5b%40name%3d%27Diabetes%20Monitoring%27%5d||chr(38)||ui.name=Diabetes%20Monitoring||chr(38)||run.outputFormat=||chr(38)||run.prompt=true||chr(38)||ui.backURL=%2fibmcognos%2fcgi-bin%2fcognos.cgi%3fb_action%3dxts.run%26m%3dportal%2fcc.xts%26m_folder%3di452592FA64524A048A981D6DD3BABBD4' cognos_dashboard_url
----------------------------------------------
FROM 
(
  SELECT 
    dt,
    a.facility_name,
    CASE
      WHEN a.facility_name = 'All Facilities'
      THEN 'HHC'
      WHEN a.facility_name = 'Coler Rehabilitation and Nursing Care Center'
      THEN 'CL'
      ELSE facility_code
    END AS facility_code,
--------------------------
    Pats_with_Diab_Schizophrenia,
    LEAD (Pats_with_Diab_Schizophrenia, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_Pats_with_Diab_Schiz,
--------------------------
    pats_with_both_results,
    LEAD (pats_with_both_results, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_pats_with_both_results,
--------------------------
    ROW_NUMBER () OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS rn
  FROM dsrip_diab_schizo a
  LEFT JOIN pt008.facility_mapping f
    ON a.facility_name = f.facility_name
  WHERE 1 = 1
  AND Pats_with_Diab_Schizophrenia <> 0
  AND pats_with_both_results <> 0
)
WHERE rn = 1 
AND EXTRACT(MONTH FROM dt) =
(
  SELECT  
    EXTRACT (MONTH FROM MAX("Report Month")) 
  FROM pt005.v_tr017_diab_mon_sum_cdw_all
);

INSERT INTO compass_metrics_beta SELECT * FROM vw_dsrip_tr017_compass;
COMMIT;
--delete from compass_metrics_beta WHERE unique_tag = 'DSRIP_DIAB_SCHRIZOPHRENIA';
SELECT * FROM compass_metrics_beta WHERE unique_tag = 'DSRIP_DIAB_SCHRIZOPHRENIA';

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--drop view vw_dsrip_bh_followup_20180511;
CREATE OR REPLACE VIEW /*vw_dsrip_bh_followup_20180511*/ vw_dsrip_tr001_compass
AS
  -- 18-May-2018, GK: Added extra date condition to result only the latest month data
  -- 15-May-2018, GK: TR001 30 and 7 Day follow-up post psychiatric hospitalization
WITH bh_vsts
AS 
(  
  SELECT
    to_date(Reporting_Month, 'MON-YYYY') AS dt,
    facility_name,
    "# Patients" AS Num_BH_Hosp_Pats,
    "# 7-day follow-up" AS NumFollowup_Vsts_In7days,
    "# 30-day follow-up" AS NumFollowup_Vsts_In30days
  FROM pt005.v_tr001_summary_cdw_all
)
SELECT 
  'BH - Followup Hospitalization Visits' box_header,
  'BH_FOLLOWUP_VISITS' unique_tag,
----------------------------
  'Total number of patients discharged from a BH Hospitalization' AS text_1,
  Num_BH_Hosp_Pats AS sub_text_1,
  NULL AS sub_text_description_1,
  prev_Num_BH_Hosp_Pats AS prev_sub_text_1,
  ROUND ( (Num_BH_Hosp_Pats - prev_Num_BH_Hosp_Pats) / Num_BH_Hosp_Pats * 100, 1) || '%' AS value1,
  'Prev month and current month variance for Number of BH Hospitalizations' AS value_desc_1,
  CASE
    WHEN ROUND ( (Num_BH_Hosp_Pats - prev_Num_BH_Hosp_Pats) / Num_BH_Hosp_Pats * 100, 0) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_1,
----------------------------------------------
  'Total number of patients who had a follow-up visit with a BH practitioner after 7 days of discharge from a BH hospitalization' AS text_2,
  NumFollowup_Vsts_In7days AS sub_text_2,
  NULL AS sub_text_description_2,
  prev_NumFollowup_Vsts_In7days AS prev_sub_text_2,
  ROUND ( (NumFollowup_Vsts_In7days - prev_NumFollowup_Vsts_In7days) / NumFollowup_Vsts_In7days * 100, 1) || '%' AS value2,
  'Prev month and current month variance for Total number of patients who had a follow-up visit with a BH practitioner after 7 days of discharge from a BH hospitalization' AS value_desc_2,
  CASE
    WHEN ROUND ( (NumFollowup_Vsts_In7days - prev_NumFollowup_Vsts_In7days) / NumFollowup_Vsts_In7days * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_2,
-----------------------------------------------
  'Total number of patients who had a follow-up visit with a BH practitioner after 30 days of discharge from a BH hospitalization' AS text_3,
  NumFollowup_Vsts_In30days AS sub_text_3,
  NULL AS sub_text_description_3,
  prev_NumFollowup_Vsts_In30days AS prev_sub_text_3,
  ROUND ( (NumFollowup_Vsts_In30days - prev_NumFollowup_Vsts_In30days) / NumFollowup_Vsts_In30days * 100, 1) || '%' AS value3,
  'Prev month and current month variance for Total number of patients who had a follow-up visit with a BH practitioner after 30 days of discharge from a BH hospitalization' AS value_desc_3,
  CASE
    WHEN ROUND ( (NumFollowup_Vsts_In30days - prev_NumFollowup_Vsts_In30days) / NumFollowup_Vsts_In30days * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END AS performance_indicator_3,
-----------------------------------------------
  NULL AS text_4,
  NULL AS sub_text_4,
  NULL AS sub_text_description_4,
  NULL AS prev_sub_text_4,
  NULL AS value4,
  NULL AS value_desc_4,
  NULL AS performance_indicator_4,
----------------------------------------------
  NULL network_name,
  facility_code,
  facility_name,
  'M' time_period_indicator,
  EXTRACT (YEAR FROM dt) yr,
  NULL quarter,
  EXTRACT (MONTH FROM dt) mnth,
  TO_CHAR (dt, 'Month') month_text,
  NULL AS date_value,
  SYSDATE create_dt,
--  'kollurug' create_by
  sys_context('USERENV', 'OS_USER') create_by,
  'http://eimtest.nychhc.org/ibmcognos/cgi-bin/cognos.cgi?b_action=cognosViewer||chr(38)||ui.action=run||chr(38)||ui.object=%2fcontent%2ffolder%5b%40name%3d%27Reports%27%5d%2ffolder%5b%40name%3d%27DSRIP%20Metrics%27%5d%2ffolder%5b%40name%3d%27DSRIP%20Metrics%27%5d%2freport%5b%40name%3d%27BH-FUH%27%5d||chr(38)||ui.name=BH-FUH||chr(38)||run.outputFormat=||chr(38)||run.prompt=true||chr(38)||ui.backURL=%2fibmcognos%2fcgi-bin%2fcognos.cgi%3fb_action%3dxts.run%26m%3dportal%2fcc.xts%26m_folder%3di452592FA64524A048A981D6DD3BABBD4' cognos_dashboard_url

----------------------------------------------
FROM 
(
  SELECT 
    dt,
    a.facility_name,
    CASE
      WHEN a.facility_name = 'ALL facilities'
      THEN 'HHC'
      WHEN a.facility_name = 'Coler Rehabilitation and Nursing Care Center'
      THEN 'CL'
      ELSE facility_code
    END AS facility_code,
--------------------------
    Num_BH_Hosp_Pats,
    LEAD (Num_BH_Hosp_Pats, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_Num_BH_Hosp_Pats,
--------------------------
    NumFollowup_Vsts_In7days,
    LEAD (NumFollowup_Vsts_In7days, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_NumFollowup_Vsts_In7days,
--------------------------
    NumFollowup_Vsts_In30days,
    LEAD (NumFollowup_Vsts_In30days, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_NumFollowup_Vsts_In30days,
--------------------------
    ROW_NUMBER () OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS rn
  FROM bh_vsts a
  LEFT JOIN pt008.facility_mapping f
    ON a.facility_name = f.facility_name
  WHERE 1 = 1
  AND Num_BH_Hosp_Pats <> 0
  AND NumFollowup_Vsts_In7days <> 0
  AND NumFollowup_Vsts_In30days <> 0
)
WHERE rn = 1
AND EXTRACT(MONTH FROM dt) = 
(
  SELECT  
    EXTRACT (MONTH FROM MAX(TO_DATE(Reporting_Month,'Mon-YYYY')))
  FROM pt005.v_tr001_summary_cdw_all
);

INSERT INTO compass_metrics_beta SELECT * FROM vw_dsrip_tr001_compass;
COMMIT;
--delete from compass_metrics_beta WHERE unique_tag = 'BH_FOLLOWUP_VISITS';
SELECT * FROM compass_metrics_beta WHERE unique_tag = 'BH_FOLLOWUP_VISITS';
  
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ 
--drop view vw_dsrip_diab_ctrl_20180511;
CREATE OR REPLACE VIEW /*vw_dsrip_diab_ctrl_20180511*/ vw_dsrip_tr002_023_compass
AS
  -- 18-May-2018, GK: Added extra date condition to result only the latest month data
  -- 16-May-2018, GK: renamed the titles 
  -- 11-May-2018, GK: TR002_023 Comprehensive Diabetes Care: Hemoglobin A1c (HbA1c) 
WITH dsrip_diab_ctrl
AS 
(  
  SELECT 
    TO_DATE ("Reporting Month", 'MON-YYYY') AS dt,
    "Facility name" AS facility_name,
    "# Patients" AS totaldiabetespat,
    "# A1c < 8" AS numpatcnt_hba1c_less8,
    "# A1c >= 9 or NULL" AS totaldiabpatpoorcntrl
  FROM pt005.v_tr002_tr023_summary_cdw_all
)
SELECT 
  'CDC - HbA1c Control' box_header,
  'DIABETES_CONTROL' unique_tag,
----------------------------
  'Number of patients whose HbA1c result from the last 12 months was less than 8%.' AS text_1,
  numpatcnt_hba1c_less8 AS sub_text_1,
  NULL AS sub_text_description_1,
  prev_numpatcnt_hba1c_less8 AS prev_sub_text_1,
  ROUND ( (numpatcnt_hba1c_less8 - prev_numpatcnt_hba1c_less8) / numpatcnt_hba1c_less8 * 100, 1) || '%' AS value1,
  'Prev month and current month variance for Number of patients in-control(HbA1c <8)' AS value_desc_1,
  CASE
    WHEN ROUND ( (numpatcnt_hba1c_less8 - prev_numpatcnt_hba1c_less8) / numpatcnt_hba1c_less8 * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_1,
-----------------------------------------------
  'Total number of patients diagnosed with diabetes who had at least one visit in the last 24 months.' AS text_2,
  totaldiabetespat AS sub_text_2,
  NULL AS sub_text_description_2,
  prev_totaldiabetespat AS prev_sub_text_2,
  ROUND ( (totaldiabetespat - prev_totaldiabetespat) / totaldiabetespat * 100, 1) || '%' AS value2,
  'Prev month and current month variance for Total number of patients diagnosed with diabetes who had at least one visit in the last 24 months.' AS value_desc_2,
  CASE
    WHEN ROUND ( (totaldiabetespat - prev_totaldiabetespat) / totaldiabetespat * 100, 0) > 0
    THEN 'BAD'
    ELSE 'GOOD'
  END performance_indicator_2,
----------------------------------------------
-------------------------------------------------
  'Number of patients whose HbA1c result from the last 12 months was greater than 9% or did not have a test done or the result was null.' AS text_3,
  totaldiabpatpoorcntrl AS sub_text_3,
  NULL AS sub_text_description_3,
  prev_totaldiabpatpoorcntrl AS prev_sub_text_3,
  ROUND ( (totaldiabpatpoorcntrl - prev_totaldiabpatpoorcntrl) / totaldiabpatpoorcntrl * 100, 1) || '%' AS value3,
  'Prev month and current month variance for Number of patients whose HbA1c result from the last 12 months was greater than 9% or did not have a test done or the result was null.' AS value_desc_3,
  CASE
    WHEN ROUND ( (totaldiabpatpoorcntrl - prev_totaldiabpatpoorcntrl) / totaldiabpatpoorcntrl * 100, 1) > 0
    THEN 'BAD'
    ELSE 'GOOD'
  END AS performance_indicator_3,
-----------------------------------------------
  NULL AS text_4,
  NULL AS sub_text_4,
  NULL AS sub_text_description_4,
  NULL AS prev_sub_text_4,
  NULL AS value4,
  NULL AS value_desc_4,
  NULL AS performance_indicator_4,
----------------------------------------------
  NULL network_name,
  facility_code,
  facility_name,
  'M' time_period_indicator,
  EXTRACT (YEAR FROM dt) yr,
  NULL quarter,
  EXTRACT (MONTH FROM dt) mnth,
  TO_CHAR (dt, 'Month') month_text,
  NULL AS date_value,
  SYSDATE create_dt,
--  'kollurug' create_by
  sys_context('USERENV', 'OS_USER') create_by,
  'http://eimtest.nychhc.org/ibmcognos/cgi-bin/cognos.cgi?b_action=cognosViewer||chr(38)||ui.action=run||chr(38)||ui.object=%2fcontent%2ffolder%5b%40name%3d%27Reports%27%5d%2ffolder%5b%40name%3d%27DSRIP%20Metrics%27%5d%2ffolder%5b%40name%3d%27DSRIP%20Metrics%27%5d%2freport%5b%40name%3d%27CDC%20%e2%80%93%20HbA1c%27%5d||chr(38)||ui.name=CDC%20%e2%80%93%20HbA1c||chr(38)||run.outputFormat=||chr(38)||run.prompt=true||chr(38)||ui.backURL=%2fibmcognos%2fcgi-bin%2fcognos.cgi%3fb_action%3dxts.run%26m%3dportal%2fcc.xts%26m_folder%3di452592FA64524A048A981D6DD3BABBD4' cognos_dashboard_url
----------------------------------------------
FROM 
(
  SELECT 
    dt,
    a.facility_name,
    CASE
      WHEN a.facility_name = 'All Facilities'
      THEN 'HHC'
      WHEN a.facility_name = 'Coler Rehabilitation and Nursing Care Center'
      THEN 'CL'
      ELSE facility_code
    END AS facility_code,
--------------------------
    totaldiabetespat,
    LEAD (totaldiabetespat, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_totaldiabetespat,
--------------------------
    numpatcnt_hba1c_less8,
    LEAD (numpatcnt_hba1c_less8, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_numpatcnt_hba1c_less8,
--------------------------
    totaldiabpatpoorcntrl,
    LEAD (totaldiabpatpoorcntrl, 1) OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS prev_totaldiabpatpoorcntrl,
--------------------------
    ROW_NUMBER () OVER (PARTITION BY a.facility_name ORDER BY dt DESC) AS rn
  FROM dsrip_diab_ctrl a
  LEFT JOIN pt008.facility_mapping f
    ON a.facility_name = f.facility_name
  WHERE 1 = 1
  AND totaldiabetespat <> 0
  AND numpatcnt_hba1c_less8 <> 0
  AND totaldiabpatpoorcntrl <> 0
)
WHERE rn = 1
AND EXTRACT (MONTH FROM dt) = 
(
  SELECT  
    EXTRACT( MONTH FROM MAX(TO_DATE("Reporting Month",'Mon-YYYY'))) 
  FROM pt005.v_tr002_tr023_summary_cdw_all
);

INSERT INTO compass_metrics_beta SELECT * FROM vw_dsrip_tr002_023_compass;
COMMIT;
--delete from compass_metrics_beta WHERE unique_tag = 'DIABETES_CONTROL';
SELECT * FROM compass_metrics_beta WHERE unique_tag = 'DIABETES_CONTROL';
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- verification SQL
--SELECT distinct box_header FROM compass_metrics_beta WHERE created_by = 'kollurug';
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
