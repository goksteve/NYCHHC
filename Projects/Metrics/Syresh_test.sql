WITH report_dates AS
      (SELECT --+ materialize
        TO_DATE(EXTRACT(YEAR FROM SYSDATE) - 1 || '-04-01', 'YYYY-MM-DD') first_rpt_dt,
        TRUNC(LAST_DAY(ADD_MONTHS(SYSDATE, -1))) last_rpt_dt --,
--        TRUNC(SYSDATE, 'MONTH') report_dt,
--        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
--        ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
       FROM
        DUAL),
     ptnt AS
      (SELECT --+ materialize
        pr.network,
        pr.patient_id,
        pat.medical_record_number AS mrn,
        pat.name patient_name,
        TRUNC(pat.birthdate) AS dob,
        pat.sex gender,
        pat.pcp_provider_name pcp,
        COUNT(*) OVER (PARTITION BY pr.patient_id, pr.network) AS single,
        ROW_NUMBER() OVER(PARTITION BY pr.patient_id, pr.network ORDER BY onset_date DESC) cnt,
        first_rpt_dt,
        last_rpt_dt
       FROM
        report_dates
        CROSS JOIN dim_patients pat
        JOIN fact_patient_diagnoses pr
         ON pr.patient_id = pat.patient_id AND pr.network = pat.network
        JOIN meta_conditions m
         ON m.VALUE = pr.diag_code AND m.criterion_id = 67
       WHERE
        pr.network = 'CBN' AND pat.current_flag = 1 AND onset_date <= last_rpt_dt),
     final AS
      (SELECT /*+ materialize full(prc) */ 
                 pat. mrn,
                 v.visit_number,
                 pat.patient_name,
                 pat.dob,
                 pat.gender,
                f.facility_name facility,
                 TRUNC(v.admission_dt) admission_date,
                 TRUNC(v.discharge_dt) discharge_date,
                 vt.name visit_type,
                 pat. pcp,
               NVL(prc.modified_proc_name, p.proc_name) document_name,
                 TO_CHAR(prc.result_dt, 'Dy, dd Mon yy') document_date,
                 TO_CHAR(prc.result_dt, 'HH24MI') document_time ,
                 CASE
                  WHEN p.src_proc_id IN (14726,
                                         14727,
                                         15123,
                                         15125)
                       AND es.name = 'complete' THEN
                   'Y'
                 END
                  AS numerator,
                 CASE WHEN single = 1 THEN 'Y' ELSE 'N' END AS singular_flag
       FROM
        ptnt pat
        JOIN fact_visits v ON v.patient_id = pat.patient_id AND v.network = pat.network
        JOIN fact_results prc ON prc.visit_id = v.visit_id AND prc.network = v.network
      JOIN dim_hc_facilities f ON f.facility_key = v.facility_key
     JOIN event_status es ON es.event_status_id = prc.event_status_id AND es.network = prc.network
      JOIN dim_procedures p ON p.proc_key = prc.proc_key
      JOIN dim_hc_departments dep ON dep.department_key = v.first_department_key
       JOIN visit_type vt ON vt.visit_type_id = v.initial_visit_type_id             AND vt.network = v.network
WHERE 
PAT.cnt = 1        
AND v.admission_dt >=  first_rpt_dt
        AND v.admission_dt <= last_rpt_dt
        AND f.facility_id IN (2, 3)
        AND f.network = 'CBN'
        AND dep.service_type = 'PCP'
)
SELECT /*+ noparallel (32)*/
 *
FROM
 final;