CREATE OR REPLACE PROCEDURE SP_STG_METRICS_DAILY_VISITS IS

/******************************************************************************
   NAME:       STG_METRICS_DAILY_VISITS
   PURPOSE:    

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        06/01/2018   goreliks1       1. Created this procedure.

  ******************************************************************************/
BEGIN
 --EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_METRICS_DAILY_VISITS';

INSERT /*+  APPEND PARALLEL(32) */
   INTO STG_METRICS_DAILY_VISITS
  (
    network,
    visit_id,
    visit_number,
    facility_id,
    facility,
    visit_type_id,
    visit_type,
    medicaid_ind,
    medicare_ind,
    patient_id,
    mrn,
    pat_lname,
    pat_fname,
    sex,
    birthdate,
    age,
    admission_dt,
    discharge_dt
  )
WITH
 get_dates
   AS
    (
      select 
      -- TO_NUMBER(TO_CHAR(TRUNC(ADD_MONTHS(SYSDATE, -1), 'MONTH'), 'yyyymmdd') || '000000') AS starting_cid,
      TO_NUMBER(TO_CHAR(SYSDATE - 10 , 'yyyymmdd') || '000000') AS starting_cid,
      TRUNC(ADD_MONTHS(SYSDATE, -1), 'MONTH') start_dt
      -- TRUNC(SYSDATE ) AS start_dt
      from dual
    ),

 v_tmp
  AS
  (
    SELECT --+ MATERIALIZE
    SUBSTR(ora_database_name, 1, 3) AS network,
    v.visit_id,
    v.visit_number,
    nvl(pe.facility_id,v.facility_id) as facility_id,
    f.name as facility,
    v.visit_type_id,
    t.name AS visit_type,
    p.patient_id,
    p.medical_record_number as mrn,
    SUBSTR(p.name, 1, INSTR(p.name, ',', 1) - 1) AS pat_lname,
    SUBSTR(p.name, INSTR(p.name, ',') + 1) AS pat_fname,
    p.sex,
    p.birthdate,
    ROUND((v.admission_date_time - p.birthdate) / 365) AS age,
    v.admission_date_time,
    v.discharge_date_time,
    row_number() over ( partition by v.visit_id order by pe.event_id ASC) as cnt
FROM get_dates cross join ud_master.visit v
    JOIN ud_master.patient p ON p.patient_id = v.patient_id
    LEFT JOIN ud_master.proc_event pe ON pe.visit_id = v.visit_id and v.facility_id is not null
    LEFT JOIN ud_master.visit_type t ON t.visit_type_id = v.visit_type_id
    LEFT JOIN ud_master.facility f  ON f.facility_id  = NVL(pe.facility_id,v.facility_id)
WHERE
    admission_date_time >= start_dt
    AND v.visit_status_id NOT IN (8,9,10,11) --REMOVE ( cancelled,closed cancelled,no show,closed no show)
    AND v.visit_type_id NOT IN (8,5,7,-1)
),
MED_TMP --+ MATERIALIZE
AS
 (
   SELECT
   visit_id, 
  CASE WHEN medicaid_ind > 0 THEN 1 ELSE 0 end as  medicaid_ind, 
  CASE WHEN medicare_ind > 0 THEN 1 ELSE 0 end as medicare_ind
  FROM
      (
        SELECT sp.visit_id, payer_type
        FROM   get_dates cross join
        ud_master.visit_segment_payer sp
        JOIN (
                SELECT  payer_id AS med_payer_id,
                CASE WHEN ( TRIM(UPPER(name)) LIKE '%MEDICAID%'   OR TRIM(UPPER(name)) LIKE 'MCAID%') THEN 'medicaid'
                when TRIM(UPPER(name)) LIKE '%MEDICARE%'   OR TRIM(UPPER(short_name)) LIKE '%MEDICARE%' then 'medicare'
                END AS  payer_type
                FROM  ud_master.payer p
                WHERE (
                       TRIM(UPPER(name)) LIKE '%MEDICAID%'  OR TRIM(UPPER(name)) LIKE 'MCAID%'
                      OR TRIM(UPPER(name)) LIKE '%MEDICARE%'  OR TRIM(UPPER(short_name)) LIKE '%MEDICARE%'
                     )  AND TRIM(UPPER(active)) = 'ACTIVE'
              ) med  ON med.med_payer_id = sp.payer_id
         JOIN ud_master.visit v on v.visit_id  = sp.visit_id
         WHERE  admission_date_time >= start_dt
         AND v.visit_status_id NOT IN (8,9,10,11) --REMOVE ( cancelled,closed cancelled,no show,closed no show)
         AND v.visit_type_id NOT IN (8,5,7,-1)
       )
      PIVOT 
          (
           count (PAYER_TYPE) as ind
          for PAYER_TYPE in( 'medicare' as medicare , 'medicaid' as medicaid)
          )
  )
SELECT /*+ PARALLEL(32) */
 v.network,
 v.visit_id,
 v.visit_number,
 v.facility_id,
 v.facility,
 v.visit_type_id,
 v.visit_type,
 NVL(m.medicaid_ind,0) as medicaid_ind,
 NVL(m.medicare_ind,0) as medicare_ind,
 v.patient_id,
 v.mrn,
 v.pat_lname,
 v.pat_fname,
 v.sex,
 v.birthdate,
 v.age,
 v.admission_date_time as admission_dt,
 v.discharge_date_time as discharge_dt
FROM
 v_tmp v
LEFT JOIN MED_TMP m on m.visit_id  = v.visit_id
LEFT JOIN ud_master.patient_secondary_number psn
ON     v.patient_id = psn.patient_id
AND psn.secondary_nbr_type_id =
CASE WHEN (v.network = 'GP1' AND v.facility_id = 1) THEN 13
WHEN (v.network = 'GP1' AND v.facility_id IN (2, 4)) THEN 11
WHEN (v.network = 'GP1' AND v.facility_id = 3) THEN 12
WHEN (v.network = 'CBN' AND v.facility_id = 4) THEN 12
WHEN (v.network = 'CBN' AND v.facility_id = 5) THEN 13
WHEN (v.network = 'NBN' AND v.facility_id = 2) THEN 9
WHEN (v.network = 'NBX' AND v.facility_id = 2) THEN 11
WHEN (v.network = 'QHN' AND v.facility_id = 2) THEN 11
WHEN (v.network = 'SBN' AND v.facility_id = 1) THEN 11
WHEN (v.network = 'SMN' AND v.facility_id = 2) THEN 11
WHEN (v.network = 'SMN' AND v.facility_id = 7) THEN 13
WHEN (v.network = 'SMN' AND v.facility_id = 8) THEN 14
WHEN (v.network = 'SMN' AND v.facility_id = 9) THEN 17
END
WHERE
 cnt = 1;

COMMIT;


   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END SP_STG_METRICS_DAILY_VISITS;



/
