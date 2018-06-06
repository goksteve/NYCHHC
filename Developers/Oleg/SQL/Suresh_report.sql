CREATE TABLE steve_del_gk_pr2
NOLOGGING
PARALLEL 32
AS
WITH 
--dt AS
--(
--  select --+ materialize 
--  TO_DATE (EXTRACT (YEAR FROM SYSDATE) - 1 || '-04-01', 'YYYY-MM-DD') begin_dt,
--  TRUNC (LAST_DAY (ADD_MONTHS (SYSDATE, -1)))      end_dt
--  from
--  dual
--),

tmp_res_field
AS
( SELECT --* materialize
                    r.network,r.visit_id, r.event_id
                    FROM
                    result r, result_field rf
                    WHERE
                    r.data_element_id = rf.data_element_id           AND r.network = rf.network
                    AND rf.name IN (
                              'Plan Of  Care',
                              '*Plan of Care',
                              'Nursing - Plan Of Care',
                              'Plan of Care',
                              'Plan Of Care Instructions',
                              'Goal for Disch Plan of Care (CCD)',
                              'Written Home Management Plan of Care Provided?',
                              'Plan Of Care',
                              'Plan Of Care Goals',
                              'Plan of Care*',
                              'Plan of Care '
                              )
                     
),

ptnt  AS 
( 
    SELECT  --+ materialize index(pr1 pk_problem) use_hash(pr pr1)
      COUNT(*) single, pr.patient_id,pr.network 
    FROM PROBLEM_CMV pr, meta_conditions mc, PROBLEM PR1
    WHERE pr1.PROBLEM_NUMBER = PR.PROBLEM_NUMBER
      AND pr.network = pr1.network
      AND Pr1.PATIENT_ID = PR.PATIENT_ID
      AND pr.code = value
      AND coding_scheme_id IN (5, 10) and CRITERION_ID=67
      AND status_id IN (0,6,7,8) and onset_date<=TRUNC (LAST_DAY (ADD_MONTHS (SYSDATE, -1))) 
     GROUP BY pr.patient_id ,pr.network
  ),   
       Final_GP1 AS
  (
   SELECT /*+ materialize  */ 
        DISTINCT NVL(pat.medical_record_number ,SECONDARY_NUMBER) MRN,
        NVL (VISIT_NUMBER, VISIT_SECONDARY_NUMBER) Visit_Number,
        pat.name patient_name,
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
       CASE WHEN v.facility_id=2 AND p.proc_id=34792    AND es.NAME='complete'  THEN 'Y'
            WHEN v.facility_id=3  AND p.proc_id=31536    AND es.NAME='complete'  THEN 'Y'
            WHEN v.facility_id=2 AND modified_proc_name='Visit Note (APC Follow-up) Medicine Primary Care - 335'
                   AND EXISTS
                   ( SELECT  'x' FROM  tmp_res_field r
                     WHERE  r.visit_id = e.visit_id AND r.event_id = e.event_id AND r.network = e.network
                   )  then 'Y'   END AS numerator, 
        CASE WHEN SINGLE =1 THEN 'Y' ELSE 'N' END AS singular_flag
    FROM 
    --  dt       CROSS  join
      visit v
      JOIN visit_type vt     ON v.visit_type_id = vt.visit_type_id AND v.network = vt.network
      JOIN ptnt ON ptnt.patient_id = v.patient_id    and v.network = ptnt.network
      JOIN proc_event prc ON v.visit_id = prc.visit_id and prc.network = v.network
      JOIN event e ON prc.event_id = e.event_id AND prc.visit_id = e.visit_id  and prc.network = e.network 
      JOIN patient pat        ON pat.patient_id = ptnt.patient_id and pat.network = ptnt.network 
      JOIN HHC_PATIENT_DIMENSION PCP        ON PCP.PATIENT_ID = pat.PATIENT_ID and PCP.network = pat.network
      JOIN FACILITY f ON v.facility_id = f.facility_id and  v.network = f.network 
      JOIN event_status es        ON e.event_status_id = es.event_status_id  and e.network = es.network
      JOIN proc P ON prc.proc_id = p.proc_id and prc.network = p.network
      LEFT JOIN visit_secondary_number vsn ON v.visit_id = vsn.visit_id AND v.NETWORK = vsn.NETWORK   
           AND vsn.visit_sec_nbr_type_id =  
                                   CASE WHEN v.facility_id = 2  AND visit_sec_nbr_nbr = 1 THEN    12
                                        WHEN v.facility_id = 3  AND visit_sec_nbr_nbr = 1 THEN    14 
                                   END
     LEFT JOIN patient_secondary_number psn ON pat.patient_id = psn.patient_id and pat.network = psn.network
           AND psn.secondary_nbr_type_id=
                                    CASE WHEN v.facility_id = 2 THEN 11
                                         WHEN v.facility_id = 3 THEN 12 
                                    END
     JOIN visit_segment_visit_location vsvl   ON     vsvl.visit_id = v.visit_id
        AND vsvl.visit_segment_number = 1 and vsvl.network = v.network
    JOIN hhc_location_dimension ld    ON ld.location_id = vsvl.location_id and ld.network = vsvl.network
    JOIN hhc_clinic_codes cc   ON cc.code = ld.clinic_code and cc.network = ld.network
   WHERE     cc.code IN ('860','861','218','223','334','335','336','337','338','339','340','341','342','346','347','348')

          AND v.ADMISSION_DATE_TIME >=    TO_DATE (EXTRACT (YEAR FROM SYSDATE) - 1 || '-04-01', 'YYYY-MM-DD') 
          AND v.ADMISSION_DATE_TIME <=    TRUNC (LAST_DAY (ADD_MONTHS (SYSDATE, -1)))
         --    AND v.ADMISSION_DATE_TIME >= dt.begin_dt    AND v.ADMISSION_DATE_TIME <=  dt.end_dt
                and v.facility_id  IN (2,3)
                and v.network='GP1' and  p.network='GP1'  and vt.network='GP1' and  f.network='GP1' and  es.network='GP1'  and 
                p.network='GP1'   and vsvl.network='GP1' and  ld.network='GP1'  and cc.network='GP1'   and pcp.network='GP1'
  )

SELECT /*+ PARALLEL (32)*/
      * FROM final_gp1
