create or replace view v_stg_patient_secondary_number
AS
SELECT 
-- '2018-Mar-22 SG SECONDARY_STAGE created
     a1.network
    ,a1.patient_id
    ,a1.visit_id
    ,a1.facility_id
    ,a1.medical_record_number
    ,a1.visit_number
FROM 
 (
  SELECT DISTINCT 
    network,
    f.patient_id, 
    f.visit_id,
    f.facility_id , 
    NVL(Patient_secondary_number,f.medical_record_number) AS medical_record_number, 
    f.visit_number
    FROM
(
    SELECT DISTINCT
    v.network,
    v.patient_id, 
    v.visit_id,
    f.facility_id ,
    p.medical_record_number,
    CASE
    WHEN (v.NETWORK = 'GP1' AND   f.facility_id =2)THEN secondary_number_1
    WHEN (v.NETWORK = 'GP1'AND    f.facility_id =1)THEN secondary_number_3
    WHEN (v.NETWORK = 'GP1'AND    f.facility_id =3)THEN secondary_number_2
    WHEN ( v.NETWORK = 'CBN'AND  f.facility_id = 4)THEN secondary_number_2
    WHEN ( v.NETWORK = 'CBN'AND  f.facility_id = 5)THEN secondary_number_3
    WHEN ( v.NETWORK = 'NBN'AND  f.facility_id = 2)THEN secondary_number_4
    WHEN ( v.NETWORK = 'NBX'AND  f.facility_id = 2)THEN secondary_number_1
    WHEN ( v.NETWORK = 'QHN'AND  f.facility_id = 2)THEN secondary_number_1
    WHEN ( v.NETWORK = 'SBN'AND  f.facility_id = 1)THEN secondary_number_1
    WHEN ( v.NETWORK= 'SMN'AND   f.facility_id = 2)THEN secondary_number_1
    WHEN ( v.NETWORK = 'SMN'AND  f.facility_id = 7)THEN secondary_number_3
    WHEN ( v.NETWORK = 'SMN'AND  f.facility_id = 8)THEN secondary_number_5
    WHEN ( v.NETWORK = 'SMN'AND  f.facility_id = 9)THEN secondary_number_6
    ELSE NULL
    END AS patient_secondary_number,
    NVL( 
        v.visit_number,
         ( 
            SELECT visit_secondary_number
            FROM   visit_secondary_number vsn
            WHERE   vsn.visit_id = v.visit_id and vsn.network =  v.network 
            AND vsn.visit_sec_nbr_type_id =
          ( CASE
              WHEN  (v.network = 'CBN'  AND f.FACILITY_ID = 4) THEN 22
              WHEN (v.network = 'CBN' AND  f.FACILITY_ID = 5) THEN 21
              WHEN (v.network = 'GP1' AND  f.FACILITY_ID = 1 AND VISIT_SEC_NBR_NBR = 1) THEN 18
              WHEN (v.network = 'GP1' AND  f.FACILITY_ID = 2 AND VISIT_SEC_NBR_NBR = 1) THEN 12
              WHEN (v.network = 'GP1' AND  f.FACILITY_ID = 3 AND VISIT_SEC_NBR_NBR = 1) THEN 14
              WHEN (v.network = 'GP2' AND  f.FACILITY_ID = 2 AND VISIT_SEC_NBR_NBR = 1) THEN 4
              WHEN (v.network = 'NBN' AND  f.FACILITY_ID IN (1, 2)) THEN 9
              WHEN (v.network = 'NBX' AND  f.FACILITY_ID = 2) THEN 13
              WHEN (v.network = 'QHN' AND  f.FACILITY_ID = 2) THEN 13
              WHEN (v.network = 'SBN' AND  f.FACILITY_ID = 1) THEN 11
              WHEN (v.network = 'SMN' AND  f.FACILITY_ID = 1) THEN 15
              WHEN (v.network = 'SMN' AND  f.FACILITY_ID = 2) THEN 12
              WHEN (v.network = 'SMN' AND  f.FACILITY_ID = 7) THEN 17
              WHEN (v.network = 'SMN' AND  f.FACILITY_ID = 8) THEN 18
              WHEN (v.network = 'SMN' AND  f.FACILITY_ID = 9) THEN 24
             END
          )
       )
       ) AS VISIT_NUMBER

    FROM
    (
     SELECT DISTINCT
       psn.network,
       psn.patient_id,
      MIN(DECODE(secondary_nbr_type_id, 11, secondary_number)) AS secondary_number_1,
      MIN(DECODE(secondary_nbr_type_id, 12, secondary_number)) AS secondary_number_2,
      MIN(DECODE(secondary_nbr_type_id, 13, secondary_number)) AS secondary_number_3,
      MIN(DECODE(secondary_nbr_type_id, 9, secondary_number)) AS secondary_number_4,
      MIN(DECODE(secondary_nbr_type_id, 14, secondary_number)) AS secondary_number_5,
      MIN(DECODE(secondary_nbr_type_id, 17, secondary_number)) AS secondary_number_6
      FROM
      patient_secondary_number psn
      GROUP BY
      psn.network, psn.patient_id
    ) A 
     JOIN fact_visits v ON v.patient_id= a.patient_id and v.network = a.network
     JOIN dim_patients p ON  p.patient_id= v.patient_id and p.network = v.network and p.current_flag  = 1
     LEFT JOIN DIM_HC_FACILITIES f ON f.facility_key  = v.facility_key
     ) F
     ) A1
    --WHERE A1.PATIENT_ID IN (113, 117)
  --  ORDER BY A1.PATIENT_ID
;
