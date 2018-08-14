CREATE OR REPLACE VIEW v_dsrip_tr002_023_a1c_cdw
AS
    WITH report_dates AS
   (
     SELECT --+ materialize
     NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')) report_dt,
     ADD_MONTHS(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), TRUNC(SYSDATE, 'MONTH')),  -24)   start_dt,
     ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) res_start_date,
     ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
    FROM
     DUAL
   ),
diab_diagnoses AS
 (
   SELECT --+ materialize
   d.network,
   d.patient_id,
   d.onset_date,
   MIN(d.onset_date) over ( PARTITION BY d.network, patient_id, include_exclude_ind ) f_onset_dt,
   MAX (d.onset_date) over ( PARTITION BY d.network, patient_id, include_exclude_ind) l_onset_dt,
   MAX (calc_excl_end_date)  over ( PARTITION BY d.network, patient_id, include_exclude_ind)  as l_end_dt,
   d.diag_code icd_code,
   d.problem_comments,
   mc.include_exclude_ind AS i_e_ind,
   ROW_NUMBER()    OVER(PARTITION BY d.network, d.patient_id, include_exclude_ind ORDER BY d.onset_date DESC) rnum
  FROM
   report_dates
   cross join   meta_conditions mc
   JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE 
   WHERE
   mc.criterion_id = 1
   AND d.status_id IN (0,6,7,8)
   AND ONSET_DATE <= report_dt
   
 ),
  tmp_res_12m AS
   (
    SELECT
     p.NETWORK,
     p.visit_id,
     p.a1c_final_result_dt as latest_result_dt,
     p.a1c_final_calc_value,
     v.patient_id,
     v.facility_key,
     v.first_payer_key AS payer_key,
     v.final_visit_type_id AS visit_type_id,
     v.financial_class_id AS plan_id,
     v.admission_dt,
     v.discharge_dt,
     ROW_NUMBER() OVER(PARTITION BY v.network, v.patient_id ORDER BY v.admission_dt DESC) res_count
    FROM
     report_dates
     CROSS JOIN   fact_visit_metric_results p 
     JOIN fact_visits v
      ON v.visit_id = p.visit_id AND v.network = p.network
    WHERE
      p.a1c_final_calc_value IS NOT NULL
     AND  v.ADMISSION_dt BETWEEN res_start_date AND report_dt
   ),
 pat_list_24m AS
 (
      SELECT
      p.*,
      rd.*,
      v.visit_id,
      v.facility_key,
      v.first_payer_key AS payer_key,
      v.final_visit_type_id AS visit_type_id,
      v.financial_class_id AS plan_id,
      v.admission_dt,
      v.discharge_dt,
      m.onset_date,
      m. f_onset_dt,
      m.l_onset_dt,
      m.l_end_dt,
      m.icd_code,
      m.problem_comments,
      ROW_NUMBER() OVER(PARTITION BY p.network, p.patient_id ORDER BY v.admission_dt DESC) visit_rnum
      FROM
      report_dates rd
      CROSS JOIN fact_visits v
      JOIN diab_diagnoses m  ON m.patient_id = v.patient_id AND m.network = v.network and rnum = 1 and i_e_ind = 'I'     
      JOIN dim_patients p  ON p.patient_id = m.patient_id  AND p.network = m.network     AND p.current_flag = 1
      AND FLOOR((rd.report_year - p.birthdate) / 365) BETWEEN 18 AND 75
      WHERE
      v.admission_dt BETWEEN rd.start_dt AND rd.report_dt
      AND v.visit_status_id NOT IN (8,9,10,11)
      AND v.initial_visit_type_id NOT IN (8,5,7,-1)
  )
SELECT --+ parallel(32)
     pp.network,
     CASE WHEN r.a1c_final_calc_value > 0 and r.a1c_final_calc_value < 8 THEN 1 ELSE NULL END AS a1c_less_8,
     CASE WHEN r.a1c_final_calc_value >= 8 THEN 1 ELSE NULL END AS a1c_more_8,
     CASE WHEN r.a1c_final_calc_value >= 9 THEN 1 ELSE NULL END AS a1c_more_9,
     CASE WHEN  NVL(r.a1c_final_calc_value, 9) >= 9 OR r.a1c_final_calc_value = 0 THEN 1 ELSE NULL END AS a1c_more_9_null,
     TO_NUMBER(TO_CHAR(NVL(r.admission_dt, pp.admission_dt), 'YYYMMDD')) AS admission_dt_key,
     NVL(r.facility_key, pp.facility_key) AS facility_key,
     f.facility_cd AS facility_code,
     f.facility_name,
     pp.patient_id,
     SUBSTR(pp.name, 1, INSTR(pp.name, ',', 1) - 1) AS pat_lname,
     SUBSTR(pp.name, INSTR(pp.name, ',') + 1) AS pat_fname,
     NVL(psn.second_mrn, pp.medical_record_number) AS mrn,
     pp.birthdate,
     ROUND((NVL(r.admission_dt, pp.admission_dt) - pp.birthdate) / 365) AS age,
      pp.apt_suite,
      pp.street_address,
      pp.city,
      pp.state,
      pp.country,
      pp.mailing_code,
      pp.home_phone,
      pp.day_phone,
      pp.pcp_provider_name AS pcp,
      nvl(r.visit_id,pp.visit_id)  AS visit_id,
      NVL(r.visit_type_id, pp.visit_type_id) AS visit_type_id,
      tt.name AS visit_type,
      NVL(r.admission_dt, pp.admission_dt) AS admission_dt,
      NVL(r.discharge_dt, pp.discharge_dt) AS discharge_dt,
      CASE UPPER(TRIM(dp.payer_group)) WHEN 'MEDICAID' THEN 'Y' ELSE NULL END AS medicaid_ind,
      (CASE
      WHEN UPPER(TRIM(dp.payer_group)) = 'MEDICAID' THEN 'Medicaid'
      WHEN UPPER(TRIM(dp.payer_group)) = 'MEDICARE' THEN 'Medicare'
      WHEN UPPER(TRIM(dp.payer_group)) = 'UNINSURED' THEN 'Self pay'
      WHEN NVL(TRIM(dp.payer_group), 'X') = 'X' THEN NULL
      ELSE 'Commercial'
      END)
      AS payer_group,
      NVL(r.payer_key, pp.payer_key) AS payer_key,
      dp.payer_name,
      NVL(r.plan_id, pp.plan_id) AS plan_id,
      fc.financial_class_name AS plan_name,
      pp.l_onset_dt as onset_dt,
      pp.icd_code,
      pp.problem_comments,
      latest_result_dt, 
      CASE WHEN r.a1c_final_calc_value < 1 THEN NULL ELSE r.a1c_final_calc_value END  as latest_result,
      'DSRIP_TR002_023' AS dsrip_report,
      pp.report_dt,
      TRUNC(SYSDATE) load_dt
   FROM
    pat_list_24m pp
    LEFT JOIN  tmp_res_12m r ON r.patient_id = pp.patient_id AND r.network = pp.network AND res_count = 1
    JOIN dim_hc_facilities f ON f.facility_key = NVL(r.facility_key, pp.facility_key)
    LEFT JOIN ref_visit_types tt ON tt.visit_type_id = NVL(r.visit_type_id, pp.visit_type_id)
    LEFT JOIN dim_payers dp ON dp.payer_key = NVL(r.payer_key, pp.payer_key)
    LEFT JOIN ref_financial_class fc
     ON fc.network = NVL(r.network, pp.network) AND fc.financial_class_id = NVL(r.plan_id, pp.plan_id)
LEFT JOIN ref_patient_secondary_mrn psn  ON   psn.NETWORK = pp.NETWORK AND psn.patient_id = pp.patient_id AND psn.facility_key = NVL(r.facility_key, pp.facility_key)
WHERE visit_rnum = 1
     AND NVL(date_of_death, date '2099-01-01') > pp.admission_dt
     AND (pp.NETWORK, pp.patient_id ) NOT  IN
                  (
                   SELECT
                   m.network, m.patient_id
                   FROM
                   pat_list_24m m
                   JOIN diab_diagnoses dd
                    ON m.network = dd.network AND m.patient_id = dd.patient_id AND i_e_ind = 'E' AND rnum = 1
                   WHERE
                   visit_rnum = 1 AND admission_dt BETWEEN dd.l_onset_dt AND ADD_MONTHS(dd.l_onset_dt, 9)
                  )

;