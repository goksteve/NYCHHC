ALTER SESSION ENABLE PARALLEL DML;
SELECT
 DISTINCT /*+ PARALLEL (4) */
          p.patient_id,
          p.medical_record_number,
          p.name,
          v.visit_id,
          v.visit_number,
          v.attending_emp_provider_id AS attending_physician_id,
          vs.admitting_emp_provider_id AS admitting_physician_id,
          vs.emp_provider_id AS performing_physician_id,
          vs.emp_provider_id AS ordering_physician_id,
          v.physician_service_id AS clinical_service_type_id,
          ms.name AS clinical_service_type,
          v.admission_date_time,
          v.discharge_date_time,
          ap.problem_number,
          DECODE(cmv.coding_scheme_id, 5, 'ICD-9', 'ICD-10') icd_code,
          b.problem_description,
          b.problem_type,
          cmv.description comments,
          v.discharge_type_id,
          dis.name AS discharge_type
FROM
 ud_master.visit v
 JOIN ud_master.patient p ON p.patient_id = v.patient_id
 JOIN ud_master.active_problem ap
  ON ap.patient_id = v.patient_id AND ap.visit_id = v.visit_id AND ap.problem_number IS NOT NULL
 JOIN ud_master.problem_cmv cmv
  ON cmv.patient_id = ap.patient_id
     AND cmv.problem_number = ap.problem_number
     AND cmv.coding_scheme_id IN (10)
 JOIN ud_master.problem b
  ON cmv.patient_id = b.patient_id AND cmv.problem_number = b.problem_number AND b.status_id IN (0,6,7,8)
 LEFT JOIN ud_master.visit_segment vs ON vs.visit_id = v.visit_id
 LEFT JOIN  ud_master.medical_specialty ms ON    v.physician_service_id = ms.physician_service_id
 LEFT JOIN ud_master.discharge_type dis ON v.discharge_type_id = dis.discharge_type_id and v.visit_type_id  = dis.visit_type_id
WHERE
 v.admission_date_time > DATE '2018-02-01'
ORDER BY PATIENT_ID, VISIT_NUMBER ,PROBLEM_NUMBER;

select * from ud_master.Visit_segment
where ADMITTING_EMP_PROVIDER_ID is not null


select * FROM PROBLEM_CMV
 
ud_master.PROC_EVENT_archive


SELECT /*+ PARALLEL(8) */
         SUBSTR(ORA_DATABASE_NAME, 1, 3),
          A.PATIENT_ID,
          A.PROBLEM_NUMBER,
          B.PROBLEM_DESCRIPTION,
          B.PROBLEM_TYPE,
          A.CODING_SCHEME_ID,
          A.CODE,
          A.DESCRIPTION,
          B.PROVISIONAL_FLAG,
          B.ONSET_DATE,
          B.START_DATE,
          B.STOP_DATE,
          B.LAST_EDIT_TIME,
          B.EMP_PROVIDER_ID,
          B.STATUS_ID,
          (
             SELECT C.NAME
               FROM UD_MASTER.PROBLEM_STATUS C
              WHERE B.STATUS_ID = C.STATUS_ID
          )
             AS PROBLEM_STATUS,
          B.PRIMARY_PROBLEM,
          B.MEDICAL_PROBLEM_FLAG,
          B.PROBLEM_LIST_TYPE_ID,
          B.PROBLEM_SEVERITY_ID
     FROM UD_MASTER.PROBLEM_CMV A
          JOIN UD_MASTER.PROBLEM B ON A.PATIENT_ID = B.PATIENT_ID AND A.PROBLEM_NUMBER = B.PROBLEM_NUMBER
    WHERE     1 = 1
          AND A.CODING_SCHEME_ID IN (5, 10)
          -- status of diagnosis must be "active".
          AND B.STATUS_ID IN (0,
                              6,
                              7,
                              8);
 select * FROM ud_master.active_problem WHERE PROBLEM_NUMBER IS  NOT NULL