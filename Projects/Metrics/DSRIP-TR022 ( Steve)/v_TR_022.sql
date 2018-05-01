--CREATE OR REPLACE VIEW V_DSRIP_TR_022_DIAB_SCREEN_CDW AS

--DIAGNOSES:DIAB MONITORING	                                 1	List of Diabetes diagnosis (monitoring)
--DIAGNOSES:NEPHROPATHY TREATMENT	                          63	List of Nephropathy Treatment diagnoses
--DIAGNOSES:KIDNEY DISEASES	                                65	List of Kidney Diseases, End-Stage Renal Diseases and Kidney Transplant diagnoses
--****************
--RESULTS:DIABETES A1C	                                     4	List of Procedures, Elements  for A1C tests
--RESULTS:NEPHROPATHY SCREEN_MONITOR	                      66	List of Nephropathy Screen Monitor tests
-- results eye exam                                         68
--********************
--MEDICATIONS:DIABETES             	                        33	List of Medications for treating Diabetes
--MEDICATIONS:ACE INHIBITOR/ARB CONTROL BLOOD PRESSURE	    64	List of Ace Inhibitor/Arb Medications to Control Blood Pressure


ALTER SESSION enable PARALLEL DML;
WITH
 report_dates AS
(
  SELECT --+ materialize
  -- ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)report_dt, 
  TRUNC(SYSDATE, 'MONTH') report_dt, 
  ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24)     start_dt,
  ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) rslt_start_date,
  ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
  FROM
  DUAL
),

pat_diag
AS
(
  select --+  materialize
  d.network, d.patient_id,
  criterion_id as crit_id,
  value as meta_value,
  include_exclude_ind as ind
  FROM   meta_conditions mc JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE
  WHERE
  mc.criterion_id IN (1,63,65)  AND d.status_id IN (0, 6, 7,8)
 ),
Visit_rslt
AS
(




pat_denom
AS
(
        SELECT --* materialize
        network, patient_id FROM pat_diag
        WHERE   crit_id = 1 AND ind = 'I'
        UNION
        SELECT  --* materialize
          d.network, d.patient_id
        FROM fact_patient_prescriptions d
        JOIN ref_drug_descriptions rd
        ON TRIM(rd.drug_description) = TRIM(d.drug_description) AND rd.drug_type_id in( 33,64)
)
,
 pat_vis_denom AS
  (
    SELECT --+  materialize 
    DISTINCT
     v.network
    ,v.visit_id
    ,v.patient_id
    ,v.facility_key
    ,v.admission_dt_key
    ,v.discharge_dt_key
    ,v.discharge_dt
    ,v.initial_visit_type_id
    ,v.visit_number
     crit_id
    FROM 
     fact_visits v 
    JOIN  pat_denom  pd on pd.patient_id = v.patient_id  and pd.network = v.network
    CROSS JOIN  report_dates d
       WHERE 
   v.admission_dt >= start_dt  AND v.admission_dt <  report_dt
--v.admission_dt >= ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24)   AND v.admission_dt <  TRUNC(SYSDATE, 'MONTH')
)
     
select --+ PARALLEL (32)
 * from pat_vis_denom


--
--ptnt_vst_type_cnt AS
--  (
--    SELECT --+ materialize
--    *
--    FROM 
--    ( 
--      (
--        SELECT 
--          network,
--          patient_id, 
--          initial_visit_type
--        FROM 
--        (
--          SELECT 
--            network,
--            patient_id,
--            initial_visit_type,
--            ROW_NUMBER() OVER (PARTITION BY network,patient_id,admission_date_time ORDER BY NULL) RN
--          FROM asthma_ptnt_lkp
--        )
--        WHERE rn = 1
--      )
--      PIVOT
--      (
--        COUNT(*) FOR initial_visit_type IN ('Clinic' clinic_visit_count, 'Emergency' emergency_visit_count,'Inpatient' inpatient_visit_count,'Outpatient' outpatient_visit_count)
--      )
--    )  
--  ),