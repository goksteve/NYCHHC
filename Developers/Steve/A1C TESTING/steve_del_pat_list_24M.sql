alter session enable parallel DDL;
alter session enable parallel DML;  
CREATE TABLE STEVE_DELE_pat_list_24m 
nologging
compress basic
parallel 32
AS
    SELECT /*+ PARALLEL (32) */ * FROM 
(     

    WITH report_dates AS
   (
     SELECT --+ materialize
     NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
     ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')),  -24)   start_dt,
     ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
     ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
    FROM
     DUAL)

SELECT
           p.*,
           rd.*,
           v.visit_id,
           v.facility_key,
           first_payer_key AS payer_key,
           final_visit_type_id AS visit_type_id,
           financial_class_id AS plan_id,
           v.admission_dt,
           v.discharge_dt,
           ROW_NUMBER() OVER(PARTITION BY p.network, p.patient_id ORDER BY v.admission_dt DESC) visit_rnum
          FROM
           report_dates rd
           CROSS JOIN fact_visits v
--           JOIN (SELECT network, patient_id
--                 FROM diab_diagnoses
--                 WHERE include_exclude_ind = 'I'
--                 MINUS
--                 SELECT network, patient_id
--                 FROM diab_diagnoses
--                 WHERE include_exclude_ind = 'E') m
--            ON m.patient_id = v.patient_id AND m.network = v.network
           JOIN dim_patients p  ON p.patient_id = v.patient_id  AND p.network = v.network     AND p.current_flag = 1
               AND FLOOR((rd.report_year - p.birthdate) / 365) BETWEEN 18 AND 75
          WHERE
           v.admission_dt BETWEEN rd.start_dt AND rd.report_dt
           AND v.visit_status_id NOT IN (8,9,10,11)
           AND v.initial_visit_type_id NOT IN (8,5,7,-1)
AND V.network = 'GP1')
where visit_rnum = 1


 ( SELECT /*+ PARALLEL(8) */
                      network,
                       visit_id,
                       patient_id,
                       visit_type_id,
                       admission_date_time,
                       discharge_date_time,
                       facility_id,
                       plan_id,
                       last_day_measur_year
                FROM  (
                          SELECT net.network,
                                 vv.visit_id,
                                 vv.patient_id,
                                 vv.visit_type_id,
                                 vv.admission_date_time,
                                 vv.discharge_date_time,
                                 vv.facility_id,
                                 vv.financial_class_id AS plan_id,
                                 net.last_day_measur_year,
                                 ROW_NUMBER() OVER(PARTITION BY vv.patient_id ORDER BY vv.admission_date_time DESC) AS cnt
                          FROM  ud_master.visit vv CROSS JOIN get_dates net
                          WHERE     1 = 1
                                AND admission_date_time BETWEEN last_24_mon AND first_day_cur_mon
                                AND vv.visit_status_id NOT IN (8,
                                                               9,
                                                               10,
                                                               11) --REMOVE ( cancelled,closed cancelled,no show,closed no show)
                                AND vv.visit_type_id NOT IN (8,
                                                             5,
                                                             7,
                                                             -1)
                      )
                WHERE cnt = 1),