drop table steve_del_cost_accounts purge ;

ALTER SESSION ENABLE PARALLEL DML;
create table steve_del_cost_accounts
nologging
compress basic
AS
WITH tmp AS
      (SELECT
        DISTINCT /*+ PARALLEL (4) */
                p.patient_id,
                 p.medical_record_number,
                 p.name,
                 v.visit_id,
                 v.visit_number,
                 ap.problem_number,
                 v.attending_emp_provider_id AS attending_physician_id,
                 MAX(vs.admitting_emp_provider_id) OVER (PARTITION BY visit_id) AS admitting_physician_id,
                 MAX(vs.emp_provider_id) OVER (PARTITION BY visit_id) AS performing_physician_id,
                 MAX(vs.emp_provider_id) OVER (PARTITION BY visit_id) AS ordering_physician_id,
                 v.physician_service_id AS clinical_service_type_id,
                 ms.name AS clinical_service_type,
                 v.admission_date_time,
                 v.discharge_date_time,
                 'ICD-10' icd_code,
                 cmv.code AS icd10_dx_cd,
                 v.discharge_type_id,
                 dis.name AS discharge_type
       FROM
        ud_master.visit v
        JOIN ud_master.patient p ON p.patient_id = v.patient_id
        JOIN ud_master.active_problem ap ON ap.patient_id = v.patient_id AND ap.visit_id = v.visit_id -- AND ap.problem_number IS NOT NULL
        JOIN ud_master.problem_cmv cmv
         ON cmv.patient_id = ap.patient_id
            AND cmv.problem_number = ap.problem_number
            AND cmv.coding_scheme_id IN (10)
        LEFT JOIN ud_master.visit_segment vs ON vs.visit_id = v.visit_id
        LEFT JOIN ud_master.medical_specialty ms ON v.physician_service_id = ms.physician_service_id
        LEFT JOIN ud_master.discharge_type dis
         ON v.discharge_type_id = dis.discharge_type_id AND v.visit_type_id = dis.visit_type_id
       WHERE
        admission_date_time > DATE '2017-12-31' AND ap.problem_number < 11)
SELECT
 *
FROM
 tmp 

PIVOT (MAX(icd10_dx_cd)
 icd10_dx_cd FOR 
problem_number IN (1,2,3,4,5,6,7,8,9,10))