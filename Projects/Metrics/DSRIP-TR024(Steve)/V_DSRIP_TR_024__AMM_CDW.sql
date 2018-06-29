CREATE OR REPLACE VIEW v_dsrip_tr_024_amm_cdw AS
 WITH report_dates AS
   (
      SELECT --+ materialize
    -- ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)report_dt,
    TRUNC(SYSDATE, 'MONTH') report_dt,
    ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) start_dt,
    ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -28) drug_calc_dt,
    --ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) rslt_start_date,
    ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
    FROM
    DUAL
   ),
  visit_pat AS
  (
    SELECT --+  materialize
    pp.network,
    admission_dt_key,
    v.visit_id,
    v.visit_number,
    v.facility_key,
    f.facility_id AS visit_facility_id,
    f.facility_name AS visit_facility_name,
    pp.patient_id,
    SUBSTR(pp.name, 1, INSTR(pp.name, ',', 1) - 1) AS pat_lname,
    SUBSTR(pp.name, INSTR(pp.name, ',') + 1) AS pat_fname,
    NVL(sec.second_mrn, pp.medical_record_number) AS mrn,
    pp.birthdate,
    ROUND((v.admission_dt - pp.birthdate) / 365) AS age,
    pp.apt_suite,
    pp.street_address,
    pp.city,
    pp.state,
    pp.country,
    pp.mailing_code,
    pp.home_phone,
    pp.day_phone,
    v.initial_visit_type_id,
    vt1.name AS initial_visit_type,
    v.final_visit_type_id AS visit_type_id,
    vt.name AS visit_type,
    v.admission_dt,
    v.discharge_dt,
    prov.physician_service_name_1 AS service,
    CASE UPPER(TRIM(pm.payer_group)) WHEN 'MEDICAID' THEN 'Y' ELSE NULL END AS medicaid_ind,
    CASE
    WHEN UPPER(TRIM(pm.payer_group)) = 'MEDICAID' THEN 'Medicaid'
    WHEN UPPER(TRIM(pm.payer_group)) = 'MEDICARE' THEN 'Medicare'
    WHEN UPPER(TRIM(pm.payer_group)) = 'UNINSURED' THEN 'Self pay'
    WHEN NVL(TRIM(pm.payer_group), 'X') = 'X' THEN NULL
    ELSE 'Commercial'
    END
    AS payer_group,
    pm.payer_id,
    pm.payer_name,
    pp.pcp_provider_name AS pcp,
    v.financial_class_id AS plan_id,
    fc.financial_class_name AS plan_name,
    d.icd_code,
    d.problem_comments,
    diagnosis_dt,
    ROW_NUMBER() OVER(PARTITION BY d.network, d.patient_id ORDER BY diagnosis_dt DESC) cnt
    FROM
    meta_conditions mc
    JOIN fact_visit_diagnoses d ON d.icd_code = mc.VALUE
    JOIN dim_patients pp ON pp.patient_id = d.patient_id AND pp.network = d.network AND current_flag = 1
    JOIN fact_visits v ON v.network = d.network AND v.visit_id = d.visit_id
    LEFT JOIN dim_hc_facilities f ON f.facility_key = v.facility_key
    LEFT JOIN dim_providers prov  ON prov.provider_key =  NVL(attending_provider_key, NVL(resident_provider_key, NVL(admitting_provider_key, 0)))
    LEFT JOIN ref_financial_class fc  ON fc.network = v.network AND fc.financial_class_id = v.financial_class_id
    LEFT JOIN ref_visit_types vt ON vt.visit_type_id = v.final_visit_type_id
    LEFT JOIN ref_visit_types vt1 ON vt1.visit_type_id = v.initial_visit_type_id
    LEFT JOIN dim_payers pm ON pm.payer_key = v.first_payer_key
    LEFT JOIN ref_patient_secondary_mrn sec ON sec.network = pp.network
    AND sec.patient_id = pp.patient_id  AND sec.facility_key = v.facility_key
    WHERE
    mc.criterion_id IN (104)
    AND diagnosis_dt >= ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24)
    AND ROUND((admission_dt - pp.birthdate) / 365) >= 18
  ),
 tmp_drug_pat AS
  (
    SELECT --+ materialize
    d.network,
    d.patient_id,
    d.drug_name,
    d.drug_description,
    d.dosage,
    d.rx_quantity,
    d.frequency,
    drug_frequency_num_val AS daily_cnt,
    TRUNC(d.order_dt) AS order_dt,
    LAG(order_dt, 1) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt)  AS prev_order_dt,
    LEAD(order_dt, 1) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS next_order_dt,
    LEAD(order_dt, 2) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS second_next_order_dt,
    LEAD(order_dt, 3) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS third_next_order_dt,
    LEAD(order_dt, 4) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS fourth_next_order_dt,
    LEAD(order_dt, 5) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS fifth_next_order_dt,
    TRUNC(order_dt) - LAG(order_dt, 1) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS diff_days,
    start_dt AS rep_start_dt
    --ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY order_dt) cnt
    FROM
    report_dates dt
    CROSS JOIN
           (
            select 
            d.network,
            d.patient_id,
            d.drug_name,
            d.drug_description,
            d.dosage,
            d.rx_quantity,
            d.frequency,
            TRUNC(d.order_dt) AS order_dt,
            ROW_NUMBER() OVER(PARTITION BY network, patient_id,  TRUNC(d.order_dt) ORDER BY TRUNC(order_dt) ASC, rx_quantity DESC) cnt
            from  fact_patient_prescriptions d
            JOIN ref_drug_descriptions rd
            ON rd.drug_description = d.drug_description AND rd.drug_type_id = 103
            AND  order_dt >=  ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -28)
          ) d
    JOIN ref_drug_descriptions rd
    ON rd.drug_description = d.drug_description AND rd.drug_type_id = 103
    JOIN ref_drug_frequency A
    ON d.frequency LIKE a.drug_frequency
    WHERE
    d.cnt = 1
   AND  order_dt >= drug_calc_dt AND order_dt < dt.report_dt
    
  ),
 drug_pat AS
 (
    SELECT --+ materialize
    network,
    patient_id,
    drug_name,
    drug_description,
    dosage,
    rx_quantity,
    frequency,
    daily_cnt,
    order_dt,
    next_order_dt,
    second_next_order_dt,
    third_next_order_dt,
    fourth_next_order_dt,
    fifth_next_order_dt
    FROM
    (
    SELECT
    network,
    patient_id,
    drug_name,
    drug_description,
    dosage,
    rx_quantity,
    frequency,
    daily_cnt,
    order_dt,
    next_order_dt,
    second_next_order_dt,
    third_next_order_dt,
    fourth_next_order_dt,
    fifth_next_order_dt,
    diff_days,
    rep_start_dt,
    ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY order_dt, rx_quantity DESC) cnt
    FROM
    tmp_drug_pat
    WHERE
    order_dt >= rep_start_dt
   )
      WHERE
      cnt = 1 AND (diff_days > 105 OR diff_days IS NULL)
),
      final_dug_pat AS
       (SELECT --+ materialize
         network,
         patient_id,
         drug_name,
         drug_description,
         dosage,
         rx_quantity,
         frequency,
         daily_cnt,
         order_dt,
         next_order_dt,
         second_next_order_dt,
         third_next_order_dt,

         CASE
          WHEN rx_quantity / daily_cnt < 85 THEN
           CASE
            WHEN rx_quantity < 1 THEN 0
            WHEN next_order_dt IS NULL THEN 0
            WHEN rx_quantity / daily_cnt > 59                 AND NVL(next_order_dt,DATE '2099-01-01')- order_dt  > 90 THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 42  AND 59   AND  NVL(next_order_dt,DATE '2099-01-01')- order_dt  > 73 THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN  27 AND 41   AND  NVL(next_order_dt,DATE '2099-01-01') - order_dt  > 57 THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 27  AND 41   AND  NVL(second_next_order_dt,DATE '2099-01-01') - order_dt   > 87  THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 19  AND 26   AND  NVL(next_order_dt,DATE '2099-01-01') - order_dt  > 50 THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 19  AND 26   AND  NVL(second_next_order_dt,DATE '2099-01-01') - order_dt   > 75  THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 19  AND 26   AND  NVL(third_next_order_dt,DATE '2099-03-01') - order_dt > 100  THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 10  AND 18   AND  NVL(next_order_dt,DATE '2099-01-01') - order_dt  > 41 THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 10  AND 18   AND  NVL(second_next_order_dt,DATE '2099-01-01') -  order_dt   > 56  THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 10  AND 18   AND  NVL(third_next_order_dt,DATE '2099-03-01') - order_dt > 71  THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 1   AND 9    AND  NVL(next_order_dt,DATE '2099-01-01') - order_dt  > 31 THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 1   AND 9    AND  NVL(second_next_order_dt,DATE '2099-01-01') - order_dt   > 46  THEN 0
            WHEN rx_quantity / daily_cnt BETWEEN 1   AND 9    AND  NVL(third_next_order_dt,DATE '2099-03-01') - order_dt > 61  THEN 0
          ELSE 1
           END
          ELSE
           1
         END
          AS numerator_flag
        FROM
         drug_pat)
 SELECT /*+ parallel(32) */
  v.network,
  v.admission_dt_key,
  v.visit_id,
  v.visit_number,
  v.facility_key,
  v.visit_facility_id,
  v.visit_facility_name,
  v.patient_id,
  v.pat_lname,
  v.pat_fname mrn,
  v.birthdate,
  v.age,
  V.apt_suite,
  v.street_address,
  v.city,
  v.state,
  v.country,
  v.mailing_code,
  v.home_phone,
  v.day_phone,
  v.initial_visit_type_id,
  v.initial_visit_type,
  v.visit_type_id,
  v.visit_type,
  v.admission_dt,
  v.discharge_dt,
  v.service,
  v.medicaid_ind,
  v.payer_group,
  v.payer_id,
  v.payer_name,
  v.pcp,
  v.plan_id,
  v.plan_name,
  v.icd_code,
  v.problem_comments,
  v.diagnosis_dt,
  p.drug_name,
  p.drug_description,
  p.order_dt,
  p.dosage,
  p.rx_quantity,
  p.frequency,
  p.daily_cnt AS daily_pills_cnt,
  p.next_order_dt,
  p.second_next_order_dt,
  p.third_next_order_dt,
  p.numerator_flag
 FROM
  visit_pat v JOIN final_dug_pat p ON p.network = v.network AND p.patient_id = v.patient_id
 WHERE
  v.cnt = 1