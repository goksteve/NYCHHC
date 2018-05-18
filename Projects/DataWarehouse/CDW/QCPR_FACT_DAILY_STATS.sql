TRUNCATE TABLE FACT_DAILY_VISITS_STATS;

INSERT /*+  APPEND PARALLEL(32) */INTO FACT_DAILY_VISITS_STATS(
    network,
    visit_id,
    visit_number,
    facility_id,
    facility,
    visit_type_id,
    visit_type,
    medicaid_ind,
    patient_id,
    mrn,
    pat_lname,
    pat_fname,
    sex,
    birthdate,
    age,
    admission_dt,
    discharge_dt)
with  v_tmp
as(
SELECT --+ materialize
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
FROM  ud_master.visit v
    JOIN ud_master.patient p ON p.patient_id = v.patient_id
    LEFT JOIN ud_master.proc_event pe ON pe.visit_id = v.visit_id and v.facility_id is not null
    LEFT JOIN ud_master.visit_type t ON t.visit_type_id = v.visit_type_id
    LEFT JOIN ud_master.facility f  ON f.facility_id  = NVL(pe.facility_id,v.facility_id)
WHERE
    admission_date_time > TRUNC(SYSDATE) - 10
    AND v.visit_status_id NOT IN (8,9,10,11) --REMOVE ( cancelled,closed cancelled,no show,closed no show)
    AND v.visit_type_id NOT IN (8,5,7,-1)
),
MED_TMP --+ materialize
AS
 (
 SELECT
  DISTINCT sp.visit_id, 1 AS medicaid_ind
  FROM
  ud_master.visit_segment_payer sp
  JOIN (SELECT
  payer_id AS med_payer_id
  FROM
  ud_master.payer
  WHERE
  (TRIM(UPPER(name)) LIKE '%MEDICAID%' OR TRIM(UPPER(name)) LIKE 'MCAID%')
  AND TRIM(UPPER(active)) = 'ACTIVE') med
  ON med.med_payer_id = sp.payer_id
)
SELECT /*+ parallel(32) */
 v.network,
 v.visit_id,
 v.visit_number,
 v.facility_id,
 v.facility,
 v.visit_type_id,
 v.visit_type,
NVL(m.medicaid_ind,0) as medicaid_ind,
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


--SELECT
--DISTINCT sp.visit_id, 1 AS medicaid_ind
--FROM
-- ud_master.visit_segment_payer sp
-- JOIN (SELECT
--   payer_id AS med_payer_id
--  FROM
--   ud_master.payer
--  WHERE
--   (TRIM(UPPER(name)) LIKE '%MEDICAID%' OR TRIM(UPPER(name)) LIKE 'MCAID%')
--   AND TRIM(UPPER(active)) = 'ACTIVE') med
--  ON med.med_payer_id = sp.payer_id