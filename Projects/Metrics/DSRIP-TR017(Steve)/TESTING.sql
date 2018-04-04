WITH report_dates AS
   (
     SELECT --+ materialize
     NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
     ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'),  -24)   start_dt,
     ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
     ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
    FROM
     DUAL
   ),

tmp_pcp_bh
AS(
 SELECT --+ parallel(32)
 v.network,
 v.visit_id,
 v.patient_id,
 f.facility_name AS pcp_bh_facility,
 v.admission_dt AS pcp_bh_visit_date,
 NVL(p.provider_name, 'Uknown') AS attending_provider,
 d.service_type AS category_id,
 ROW_NUMBER() OVER(PARTITION BY vs.visit_id, d.service_type ORDER BY v.admission_dt DESC) cnt
FROM
 dim_hc_departments d
 JOIN stg_visit_segment_locations vs
  ON d.location_id = vs.location_id AND d.network = vs.network AND d.service_type IN ('PCP', 'BH')
 JOIN fact_visits v ON v.visit_id = vs.visit_id AND v.network = vs.network
 JOIN dim_hc_facilities f ON f.facility_key = d.facility_key
 LEFT JOIN dim_providers p ON p.provider_key = v.attending_provider_key
WHERE
 v.admission_dt >= ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) AND v.admission_dt < TRUNC(SYSDATE, 'MONTH')

) ,




last_pcp_bh AS
           ( SELECT network,
                    visit_id,
                    facility,
                    patient_id,
                    TRUNC(last_visit_date) AS last_visit_date,
                    provider_id,
                    NVL(ep.name, 'Uknown') AS provider,
                    category_id
             FROM  ( SELECT b.network,
                            a.visit_id,
                            b.facility,
                            a.patient_id,
                            a.admission_date_time AS last_visit_date,
                            a.attending_emp_provider_id AS provider_id,
                            b.category_id,
                            ROW_NUMBER() OVER(PARTITION BY a.patient_id, b.category_id ORDER BY a.admission_date_time DESC) rnk
                     FROM  (   SELECT DISTINCT patient_id  FROM  res_tmp ) v
                           JOIN ud_master.visit a ON     a.patient_id = v.patient_id   AND a.admission_date_time BETWEEN ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) AND TRUNC(SYSDATE, 'MONTH')
                           JOIN tmp_pcp_bh   b           ON b.visit_id = a.visit_id and ) a
                   LEFT JOIN ud_master.emp_provider ep ON a.provider_id = ep.emp_provider_id
             WHERE rnk = 1)

select * from last_pcp_bh

DIM_HC_DEPARTMENTS