--Diabetes Monitoring for People with Diabetes and Schizophrenia.
CREATE OR REPLACE VIEW vw_dsrip_diab_schizo
AS
--  GK, 14-May-2018
WITH dsrip_diab_schizo
AS 
(  
  SELECT 
    to_date(TO_CHAR ("Report Month", 'MON-YYYY'), 'MON-YYYY')  AS dt,
    "Facility Name" AS facility_name,
    "# Patients" AS Pats_with_Diab_Schizophrenia,
    "# Patient with Both Results" pats_with_both_results
  FROM pt005.v_tr017_diab_mon_sum_cdw_all
  ORDER BY TO_CHAR ("Report Month", 'MON-YYYY') DESC
)
SELECT 
  'DSRIP report - Diabetes Monitoring for People with Diabetes and Schizophrenia.' box_header,
  'DSRIP_DIAB_SCHRIZOPHRENIA' unique_tag,
----------------------------
  'Total no. Diabetes patients' AS text_1,
  Pats_with_Diab_Schizophrenia AS sub_text_1,
  prev_Pats_with_Diab_Schiz AS prev_sub_text_1,
  ROUND ( (Pats_with_Diab_Schizophrenia - prev_Pats_with_Diab_Schiz) / Pats_with_Diab_Schizophrenia * 100, 1) || '%' AS value1,
  'Prev month and current month variance for Total no. of Patients with Diabetes and Schizophrenia' AS value_desc_1,
  CASE
    WHEN ROUND ( (Pats_with_Diab_Schizophrenia - prev_Pats_with_Diab_Schiz) / Pats_with_Diab_Schizophrenia * 100, 0) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_1,
----------------------------------------------
  'Number of Diabetes Patients with Both LDL and A1C Results' AS text_2,
  pats_with_both_results AS sub_text_2,
  prev_pats_with_both_results AS prev_sub_text_2,
  ROUND ( (pats_with_both_results - prev_pats_with_both_results) / pats_with_both_results * 100, 1) || '%' AS value2,
  'Prev month and current month variance for Total Number of Followup Visits within 7 Days' AS value_desc_2,
  CASE
    WHEN ROUND ( (pats_with_both_results - prev_pats_with_both_results) / pats_with_both_results * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_2,
-----------------------------------------------
  NULL AS text_3,
  NULL AS sub_text_3,
  NULL AS prev_sub_text_3,
  NULL AS value3,
  NULL AS value_desc_3,
  NULL AS performance_indicator_3,
-----------------------------------------------
  NULL AS text_4,
  NULL AS sub_text_4,
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
  'kollurug' create_by
----------------------------------------------
FROM 
(
--SELECT * FROM (
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
    --network,
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
--  and a.dt = date '2018-01-01'
--  and a.facility_name =  'Kings County Hospital Center'
--  AND dt = TRUNC( ADD_MONTHS( SYSDATE, -1), 'MONTH')
  AND Pats_with_Diab_Schizophrenia <> 0
  AND pats_with_both_results <> 0
)
WHERE rn = 1;

INSERT INTO compass_metrics SELECT * FROM vw_dsrip_diab_schizo;
COMMIT;
--delete from compass_metrics WHERE unique_tag = 'DSRIP_BH_Followup_Visits';
SELECT * FROM compass_metrics WHERE unique_tag = 'DSRIP_BH_Followup_Visits';















select * from vw_dsrip_bh_followup_20180511;



--BH - Follow-up visit post BH Hospitalization
CREATE OR REPLACE VIEW vw_dsrip_bh_followup_20180511
AS
--  GK, 11-May-2018
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
  ORDER BY to_date(Reporting_Month, 'MON-YYYY') desc
)
SELECT 
  'DSRIP report - Follow-up visit post BH Hospitalization' box_header,
  'DSRIP_BH_Followup_Visits' unique_tag,
----------------------------
  'Total no. of BH Hospitalizations' AS text_1,
  Num_BH_Hosp_Pats AS sub_text_1,
  prev_Num_BH_Hosp_Pats AS prev_sub_text_1,
  ROUND ( (Num_BH_Hosp_Pats - prev_Num_BH_Hosp_Pats) / Num_BH_Hosp_Pats * 100, 1) || '%' AS value1,
  'Prev month and current month variance for Total no. of BH Hospitalizations' AS value_desc_1,
  CASE
    WHEN ROUND ( (Num_BH_Hosp_Pats - prev_Num_BH_Hosp_Pats) / Num_BH_Hosp_Pats * 100, 0) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_1,
----------------------------------------------
  'Number of Followup Visits within 7 Days' AS text_2,
  NumFollowup_Vsts_In7days AS sub_text_2,
  prev_NumFollowup_Vsts_In7days AS prev_sub_text_2,
  ROUND ( (NumFollowup_Vsts_In7days - prev_NumFollowup_Vsts_In7days) / NumFollowup_Vsts_In7days * 100, 1) || '%' AS value2,
  'Prev month and current month variance for Total Number of Followup Visits within 7 Days' AS value_desc_2,
  CASE
    WHEN ROUND ( (NumFollowup_Vsts_In7days - prev_NumFollowup_Vsts_In7days) / NumFollowup_Vsts_In7days * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_2,
-----------------------------------------------
  'Number of Followup Visits within 30 Days' AS text_3,
  NumFollowup_Vsts_In30days AS sub_text_3,
  prev_NumFollowup_Vsts_In30days AS prev_sub_text_3,
  ROUND ( (NumFollowup_Vsts_In30days - prev_NumFollowup_Vsts_In30days) / NumFollowup_Vsts_In30days * 100, 1) || '%' AS value3,
  'Prev month and current month variance for Total Number of Followup Visits within 30 Days)' AS value_desc_3,
  CASE
    WHEN ROUND ( (NumFollowup_Vsts_In30days - prev_NumFollowup_Vsts_In30days) / NumFollowup_Vsts_In30days * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END AS performance_indicator_3,
-----------------------------------------------
  NULL AS text_4,
  NULL AS sub_text_4,
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
  'kollurug' create_by
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
    --network,
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
--  and a.dt = date '2018-01-01'
--  and a.facility_name =  'Kings County Hospital Center'
--  AND dt = TRUNC( ADD_MONTHS( SYSDATE, -1), 'MONTH')
  AND Num_BH_Hosp_Pats <> 0
  AND NumFollowup_Vsts_In7days <> 0
  AND NumFollowup_Vsts_In30days <> 0
)
WHERE rn = 1;

INSERT INTO compass_metrics SELECT * FROM vw_dsrip_bh_followup_20180511;
COMMIT;
--delete from compass_metrics WHERE unique_tag = 'DSRIP_BH_Followup_Visits';
SELECT * FROM compass_metrics WHERE unique_tag = 'DSRIP_BH_Followup_Visits';
  
 


--Diabetes Care – HbA1c Control
CREATE OR REPLACE VIEW vw_dsrip_diab_ctrl_20180511
AS
--  GK, 11-May-2018
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
  ORDER BY TO_DATE ("Reporting Month", 'MON-YYYY') DESC
)
SELECT 
  'DSRIP report - Diabetes HbA1c Control' box_header,
  'DSRIP_DIAB_CONTROL' unique_tag,
----------------------------
  'Total no. of patients with diabetes' AS text_1,
  totaldiabetespat AS sub_text_1,
  prev_totaldiabetespat AS prev_sub_text_1,
  ROUND ( (totaldiabetespat - prev_totaldiabetespat) / totaldiabetespat * 100, 1) || '%' AS value1,
  'Prev month and current month variance for Total no. of patients with diabetes' AS value_desc_1,
  CASE
    WHEN ROUND ( (totaldiabetespat - prev_totaldiabetespat) / totaldiabetespat * 100, 0) > 0
    THEN 'BAD'
    ELSE 'GOOD'
  END performance_indicator_1,
----------------------------------------------
  'Total no. patients in control (HbA1c <8)' AS text_2,
  numpatcnt_hba1c_less8 AS sub_text_2,
  prev_totaldiabetespat AS prev_sub_text_2,
  ROUND ( (numpatcnt_hba1c_less8 - prev_numpatcnt_hba1c_less8) / numpatcnt_hba1c_less8 * 100, 1) || '%' AS value2,
  'Prev month and current month variance for Total no. patients in control (HbA1c < 8)' AS value_desc_2,
  CASE
    WHEN ROUND ( (numpatcnt_hba1c_less8 - prev_numpatcnt_hba1c_less8) / numpatcnt_hba1c_less8 * 100, 1) > 0
    THEN 'GOOD'
    ELSE 'BAD'
  END performance_indicator_2,
-----------------------------------------------
  'Total no. of patients in poor control (HbA1c >=9 or no test values)' AS text_3,
  totaldiabpatpoorcntrl AS sub_text_3,
  prev_totaldiabetespat AS prev_sub_text_3,
  ROUND ( (totaldiabpatpoorcntrl - prev_totaldiabpatpoorcntrl) / totaldiabpatpoorcntrl * 100, 1) || '%' AS value3,
  'Prev month and current month variance for Total no. of patients in poor control (HbA1c >=9 or no test values)' AS value_desc_3,
  CASE
    WHEN ROUND ( (totaldiabpatpoorcntrl - prev_totaldiabpatpoorcntrl) / totaldiabpatpoorcntrl * 100, 1) > 0
    THEN 'BAD'
    ELSE 'GOOD'
  END AS performance_indicator_3,
-----------------------------------------------
  NULL AS text_4,
  NULL AS sub_text_4,
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
  'kollurug' create_by
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
    --network,
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
  --and dt >= date '2018-01-01'
  --and a.facility_name =  'Kings County Hospital Center'
--  AND dt = TRUNC( ADD_MONTHS( SYSDATE, -1), 'MONTH')
  AND totaldiabetespat <> 0
  AND numpatcnt_hba1c_less8 <> 0
  AND totaldiabpatpoorcntrl <> 0
)
WHERE rn = 1;
--AND EXTRACT (YEAR FROM dt) = 2018 AND EXTRACT (MONTH FROM dt) = 4;

INSERT INTO compass_metrics SELECT * FROM vw_dsrip_diab_ctrl_20180511;
COMMIT;

SELECT * FROM compass_metrics WHERE unique_tag = 'DSRIP_DIAB';