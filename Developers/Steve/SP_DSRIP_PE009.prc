CREATE OR REPLACE PROCEDURE sp_dsrip_PE009 AS
-- 2018-MAR-23 SG Create
BEGIN

 EXECUTE IMMEDIATE 'ALTER SESSION enable parallel DML';

 EXECUTE IMMEDIATE 'TRUNCARTE TABLE FINAL_TABLE';

--************ 1  CBN *************************
INSERT /*+ APPEND PARALLEL(32 */
 INTO FINAL_TABLE
(

---table columns
)

with 
ptnt  AS 
  ( 
    SELECT  --+ materialize index(pr1 pk_problem) use_hash(pr pr1)
      COUNT (*) single, pr.patient_id,pr.network 
    FROM PROBLEM_CMV partition(CBN) pr, meta_conditions, PROBLEM partition(CBN) PR1
    WHERE  pr1.PROBLEM_NUMBER = PR.PROBLEM_NUMBER
                  AND pr.network = pr1.network
                  AND Pr1.PATIENT_ID = PR.PATIENT_ID
                  AND pr.code = value
                  AND coding_scheme_id IN (5, 10) AND CRITERION_ID=67
                  AND status_id IN (0,6,7,8) AND onset_date<=TRUNC (LAST_DAY (ADD_MONTHS (SYSDATE, -1))) 
         GROUP BY pr.patient_id ,pr.network
  ),
cbn AS 
(
  SELECT  --+ materialize
    DISTINCT pat.medical_record_number AS MRN,
    visit_number,
    pat.name AS patient_name,
    TRUNC (pat.birthdate) AS DOB,
    pat.sex Gender,
    f.name FACILITY,
    TRUNC (ADMISSION_DATE_TIME) ADMISSION_DATE,
    TRUNC (DISCHARGE_DATE_TIME) DISCHARGE_DATE,
    vt.name VISIT_TYPE,
    PCP.PRIM_CARE_PROVIDER PCP,
    NVL (prc.modified_proc_name, p.name) Document_Name,
    TO_CHAR (e.date_time, 'Dy, dd Mon yy') Document_Date,
    TO_CHAR (e.date_time, 'HH24MI') Document_Time,
    case when p.proc_id in (14726 ,14727 ,15123 ,15125) AND es.name='complete'  then 'Y'  end as Numerator,
    case when single =1 then 'Y' else 'N' end as Singular_Flag
  FROM 
   visit partition(CBN) v 
    JOIN visit_type  vt
    ON v.visit_type_id = vt.visit_type_id AND v.network = vt.network
  JOIN ptnt ON ptnt.patient_id = v.patient_id    AND v.network = ptnt.network
  JOIN proc_event partition(CBN) prc ON v.visit_id = prc.visit_id AND prc.network = v.network
  JOIN event partition(CBN) E
    ON prc.event_id = e.event_id AND prc.visit_id = e.visit_id  AND prc.network = e.network 
  JOIN patient  partition(CBN) pat
    ON pat.patient_id = ptnt.patient_id AND pat.network = ptnt.network 
  JOIN HHC_PATIENT_DIMENSION PCP
    ON PCP.PATIENT_ID = pat.PATIENT_ID AND PCP.network = pat.network
  JOIN FACILITY f ON v.facility_id = f.facility_id AND  v.network = f.network 
  JOIN event_status  es
    ON e.event_status_id = es.event_status_id  AND e.network = es.network
  JOIN proc partition(CBN) P ON prc.proc_id = p.proc_id AND prc.network = p.network
  JOIN visit_segment_visit_location partition(CBN) vsvl
    ON vsvl.visit_id = v.visit_id
   AND vsvl.visit_segment_number = 1 AND vsvl.network = v.network
  JOIN hhc_location_dimension ld
   ON ld.location_id = vsvl.location_id AND ld.network = vsvl.network
  JOIN hhc_clinic_codes cc
   ON cc.code = ld.clinic_code AND cc.network = ld.network
  WHERE cc.code IN ('860','861','218','223','334','335','336','337','338','339','340','341','342','346','347','348')
    AND TRUNC (ADMISSION_DATE_TIME) >= TO_DATE (EXTRACT (YEAR FROM SYSDATE) - 1 || '-04-01', 'YYYY-MM-DD')
    AND TRUNC (ADMISSION_DATE_TIME) <=TRUNC (LAST_DAY (ADD_MONTHS (SYSDATE, -1)))    
    AND v.facility_id in (2,3)
)
select * from cbn;

COMMIT;

--**************** 2 NBN  *********************
--**************** 3 NBX *********************
--**************** 4 SMN*********************
--**************** 5 GP1  *********************

  COMMIT;
EXCEPTION
   WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('The error code is ' || ERROR_STEP || '-' || SQLCODE || '- ' || SUBSTR(SQLERRM, 1, 64));
END;
/
