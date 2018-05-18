CREATE OR REPLACE FORCE VIEW v_dsrip_tr_013_014_asthma_cdw
(
 report_run_dt,
 begin_dt,
 end_dt,
 network,
 facility_name,
 patient_id,
 visit_id,
 patient_name,
 mrn,
 apt_suite,
 street_address,
 city,
 state,
 country,
 mailing_code,
 home_phone,
 birthdate,
 age,
 pcp_id,
 pcp_name,
 last_pcp_visit_id,
 last_pcp_visit_dt,
 final_visit_type,
 initial_visit_type,
 admission_dt,
 discharge_dt,
 payer_name,
 payer_group,
 plan_name,
 medicaid_ind,
 icd_code,
 problem_description,
 asthma_erlst_med_name,
 route,
 dosage,
 frequency,
 asthma_earlst_rx_dt,
 sum_days_covered_in_yr,
 treatment_days_in_year,
 proportion_days_covered,
 numeraor_flag_75_med_ratio,
 numeraor_flag_50_med_ratio
) AS
 WITH dt AS
       (SELECT --+ materialize
         TRUNC(SYSDATE, 'MONTH') AS report_run_dt,
         TRUNC(SYSDATE, 'YEAR') AS msrmnt_yr,
         ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) begin_dt,
         TRUNC(SYSDATE, 'MONTH') - 1 end_dt
        --      DATE '2016-02-01' begin_dt,
        --      DATE '2018-03-31' end_dt
        FROM
         DUAL),
      asthma_ptnt_lkp AS
       (SELECT --+ materialize
         a.network,
         a.report_run_dt,
         a.begin_dt,
         a.end_dt,
         a.patient_id,
         a.visit_id,
         a.facility_key,
         a.facility_name,
         a.final_visit_type_id,
         a.final_visit_type,
         a.initial_visit_type_id,
         a.initial_visit_type,
         a.admission_dt,
         a.discharge_dt,
         a.icd_code,
         a.problem_description,
         a.coding_scheme,
         a.first_payer_key,
         a.financial_class_id,
         a.ptnt_prb_rnum,
         a.ptnt_vst_rnum,
         ROW_NUMBER() OVER(PARTITION BY a.network, a.patient_id ORDER BY a.admission_dt DESC) ltst_ptnt_rec
        FROM
         (SELECT --+ materialize
           DISTINCT
           vst.network,
           dt.report_run_dt,
           dt.begin_dt,
           dt.end_dt,
           vst_prb.patient_id,
           vst_prb.visit_id,
           vst.facility_key,
           fclty.facility_name,
           vst.final_visit_type_id,
           vt1.name final_visit_type,
           vst.initial_visit_type_id,
           vt2.name initial_visit_type,
           vst.admission_dt,
           vst.discharge_dt,
           vst_prb.icd_code,
           vst_prb.problem_comments problem_description,
           vst_prb.coding_scheme,
           vst.first_payer_key,
           vst.financial_class_id,
           ROW_NUMBER()
           OVER(
            PARTITION BY vst_prb.network, vst_prb.patient_id
            ORDER BY vst.admission_dt DESC, vst_prb.coding_scheme ASC)
            ptnt_prb_rnum,
           ROW_NUMBER()
           OVER(
            PARTITION BY vst_prb.network, vst_prb.patient_id, vst.visit_id
            ORDER BY vst.admission_dt DESC, vst_prb.coding_scheme ASC)
            ptnt_vst_rnum
          FROM
           dt
           JOIN fact_visits /*ud_master.visit*/
                           vst ON vst.admission_dt BETWEEN dt.begin_dt AND dt.end_dt --AND vst.patient_id = 1572634 AND vst.network = 'CBN'
           JOIN fact_visit_diagnoses /*goreliks1.visit_event_icd_code*/
                                    vst_prb --TABLE WITH VISIT AND DIAGNOSIS RELATIONSHIP
            ON vst_prb.visit_id = vst.visit_id AND vst_prb.network = vst.network
           JOIN meta_conditions metac -- ASTHMA DIAGNOSIS CODE METADATA
            ON metac.VALUE = vst_prb.icd_code
               AND metac.criterion_id = 21
               AND include_exclude_ind = 'I'
               AND metac.network = 'ALL'
           LEFT JOIN ref_visit_types vt1 ON vt1.visit_type_id = vst.final_visit_type_id
           LEFT JOIN dim_hc_facilities fclty ON fclty.facility_key = vst.facility_key
           LEFT JOIN ref_visit_types vt2
            ON vt2.visit_type_id = vst.initial_visit_type_id
               AND NOT EXISTS
                    (SELECT
                      cmv.patient_id, cmv.network
                     FROM
                      fact_patient_diagnoses cmv JOIN meta_conditions metac1 ON metac1.VALUE = cmv.diag_code
                     WHERE
                      metac1.criterion_id = 21
                      AND metac1.include_exclude_ind = 'E'
                      AND metac.network = 'ALL'
                      AND cmv.patient_id = vst_prb.patient_id
                      AND cmv.network = vst_prb.network)) a
        WHERE
         a.ptnt_vst_rnum = 1),
      pcp_vst_dt AS
       (SELECT --+ materialize
         vst_lkp.network,
         vst_lkp.patient_id,
         vst_lkp.visit_id,
         vst_lkp.admission_dt,
         ROW_NUMBER()
          OVER(PARTITION BY vst_lkp.network, vst_lkp.patient_id ORDER BY vst_lkp.admission_dt DESC)
          pcp_visit_dt_rnum
        FROM
         asthma_ptnt_lkp vst_lkp
         JOIN fact_visits vst ON vst.patient_id = vst_lkp.patient_id AND vst.network = vst_lkp.network --AND vst.patient_id = 1572634 AND vst.network = 'CBN'
         JOIN dim_hc_departments pcp
          ON pcp.department_key = vst.last_department_key AND pcp.service_type = 'PCP'),
      ----temp table to count different visit types per patient, this is useful to determine persistent asthma
      ptnt_vst_type_cnt AS
       (SELECT --+ materialize
         *
        FROM
         ((
           SELECT
            network, patient_id, initial_visit_type
           FROM
            (
             SELECT
              network,
              patient_id,
              initial_visit_type,
              ROW_NUMBER() OVER(PARTITION BY network, patient_id, admission_dt ORDER BY NULL) rn
             FROM
              asthma_ptnt_lkp
            )
           WHERE
            rn = 1
          )
          PIVOT
           (COUNT(*)
           FOR initial_visit_type
           IN ('Clinic' clinic_visit_count,
              'Emergency' emergency_visit_count,
              'Inpatient' inpatient_visit_count,
              'Outpatient' outpatient_visit_count)))),
      --temp table to retrieve leukotriene modifiers and antibody inhibitors prescriptions for the identified patients using metadata provided by BA
      med_luk_antbdy_dspns_evnt AS
       (SELECT --+ materialize
         network,
         report_run_dt,
         begin_dt,
         end_dt,
         patient_id,
         COUNT(*) OVER (PARTITION BY network, patient_id) med_luk_antbdy_dspns_evnt_cnt,
         ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY rx_order_time DESC) ltst_luk_antbdy_rnum
        FROM
         (SELECT
           DISTINCT rx.network,
                    rx.patient_id,
                    rx.order_dt rx_order_time,
                    rx_exp_dt rx_exp_time,
                    rx.drug_name derived_product_name,
                    rx.rx_quantity,
                    rx.rx_dc_dt rx_dc_time,
                    dt.report_run_dt,
                    dt.begin_dt,
                    dt.end_dt
          FROM
           dt
           JOIN fact_patient_prescriptions rx ON rx.order_dt BETWEEN dt.begin_dt AND dt.end_dt --AND rx.patient_id = 1572634 AND rx.network = 'CBN'
           JOIN ref_drug_descriptions rd
            ON rx.drug_description = rd.drug_description AND rd.drug_type_id IN (40, 41))),
      --  SELECT --+ parallel(32)
      --  * FROM med_luk_antbdy_dspns_evnt;

      --temp table to retrieve asthma controller medication prescriptions for the identified patients using metadata provided by BA
      asthma_cntrlr_med_dspns_evnt AS
       (SELECT --+ materialize
         network,
         patient_id,
         rx_order_time,
         derived_product_name,
         COUNT(*) OVER (PARTITION BY network, patient_id) asthma_cntrlr_med_cnt,
         ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY rx_order_time DESC) ltst_cntrlr_med_rnum
        FROM
         (SELECT
           DISTINCT rx.network,
                    rx.patient_id,
                    rx.order_dt rx_order_time,
                    rx_exp_dt rx_exp_time,
                    rx.drug_name derived_product_name,
                    rx.rx_quantity,
                    rx.rx_dc_dt rx_dc_time
          FROM
           dt
           JOIN fact_patient_prescriptions rx ON rx.order_dt BETWEEN dt.begin_dt AND dt.end_dt --AND rx.patient_id = 1572634 AND rx.network = 'CBN'
           JOIN ref_drug_descriptions rd
            ON rx.drug_description = rd.drug_description AND rd.drug_type_id = 42)),
      --temp table to retrieve asthma other medication prescriptions for the identified patients using metadata provided by BA
      asthma_other_med_dspns_evnt AS
       (SELECT --+ materialize
         network,
         patient_id,
         rx_order_time,
         derived_product_name,
         COUNT(*) OVER (PARTITION BY network, patient_id) asthma_other_med_cnt,
         ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY rx_order_time DESC) ltst_other_med_rnum
        FROM
         (SELECT
           DISTINCT rx.network,
                    rx.patient_id,
                    rx.order_dt rx_order_time,
                    rx_exp_dt rx_exp_time,
                    rx.drug_name derived_product_name,
                    rx.rx_quantity,
                    rx.rx_dc_dt rx_dc_time
          FROM
           dt
           JOIN fact_patient_prescriptions rx ON rx.order_dt BETWEEN dt.begin_dt AND dt.end_dt --AND rx.patient_id = 1572634 AND rx.network = 'CBN'
           JOIN ref_drug_descriptions rd
            ON rx.drug_description = rd.drug_description AND rd.drug_type_id = 43)),
      medication_mngmnt AS
       (SELECT --+ materialize
         network,
         patient_id,
         order_dt,
         rx_yr_end_dt,
         drug_name,
         drug_description,
         rx_quantity,
         frequency,
         drug_frequency_num_val,
         route,
         dosage,
         dosage_num_val,
         CASE
          WHEN route = 'inhalation' THEN rx_quantity / (dosage_num_val * drug_frequency_num_val) --dosage/num_val
          ELSE rx_quantity / drug_frequency_num_val
         END
          days_covered_per_rx,
         ROUND(
          CASE
           WHEN EXTRACT(YEAR FROM order_dt) = EXTRACT(YEAR FROM rx_yr_end_dt) THEN
            SUM(
             CASE
              WHEN route = 'inhalation' THEN rx_quantity / (dosage_num_val * drug_frequency_num_val) --dosage/num_val
              ELSE rx_quantity / drug_frequency_num_val
             END)
            OVER(PARTITION BY network, patient_id, EXTRACT(YEAR FROM order_dt) ORDER BY NULL)
          END)
          AS sum_days_covered_in_yr,
         (  rx_yr_end_dt
          - MIN(order_dt)
             OVER(PARTITION BY network, patient_id, EXTRACT(YEAR FROM order_dt) ORDER BY order_dt))
          treatment_days_in_year,
         MIN(order_dt) OVER(PARTITION BY network, patient_id ORDER BY order_dt) asthma_earlst_rx_dt,
         ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY order_dt) asthma_earlst_rx_rnum
        FROM
         (SELECT --+ parallel(8)
           DISTINCT rx.network,
                    rx.patient_id,
                    rx.order_dt,
                    ADD_MONTHS(TRUNC(order_dt, 'YEAR'), 12) - 1 rx_yr_end_dt,
                    rx.drug_name,
                    rx.drug_description,
                    rx.rx_quantity,
                    rx.dosage,
                    rx.frequency,
                    NVL(t2.drug_frequency_num_val, 1) drug_frequency_num_val,
                    NVL(t3.drug_frequency_num_val, 1) dosage_num_val,
                    rx.rx_exp_dt,
                    rd.route
          FROM
           fact_patient_prescriptions rx
           JOIN pt005.tr13_ref_drug_descriptions rd
            ON rx.drug_description = rd.drug_description --AND rx.patient_id = 1572634 AND rx.network = 'CBN'
                                                        AND rd.drug_type_id = 44 --Number of people who achieved a proportion of days covered for their asthma controller medications, hence only criterion_id 44
           JOIN asthma_ptnt_lkp a
            ON a.patient_id = rx.patient_id
               AND a.network = rx.network
               AND a.ltst_ptnt_rec = 1
               AND rx.order_dt BETWEEN a.begin_dt AND a.end_dt
           LEFT JOIN pt005.ganesh_ref_drug_frequency t2
            ON LOWER(rx.frequency) LIKE t2.drug_frequency AND t2.med_route = 'oral'
           LEFT JOIN pt005.ganesh_ref_drug_frequency t3
            ON LOWER(rx.dosage) LIKE t3.drug_frequency AND t3.med_route = 'inhalation'))
 SELECT --+ parallel(32)
  DISTINCT
  c.report_run_dt,
  c.begin_dt,
  c.end_dt,
  a.network,
  a.facility_name,
  a.patient_id,
  a.visit_id,
  ptnt.name patient_name,
  --  NVL(psn.medical_record_number, ptnt.medical_record_number) AS mrn,
  NVL(ptnt.medical_record_number, mdm.mrn) mrn,
  ptnt.apt_suite,
  ptnt.street_address,
  ptnt.city,
  ptnt.state,
  ptnt.country,
  ptnt.mailing_code,
  ptnt.home_phone,
  ptnt.birthdate,
  FLOOR((ADD_MONTHS(TRUNC(SYSDATE, 'year'), 12) - 1 - ptnt.birthdate) / 365) age,
  ptnt.pcp_provider_id pcp_id,
  ptnt.pcp_provider_name pcp_name,
  f.visit_id last_pcp_visit_id,
  f.admission_dt last_pcp_visit_dt,
  a.final_visit_type,
  a.initial_visit_type,
  a.admission_dt,
  a.discharge_dt,
  pm.payer_name,
  pm.payer_group,
  fin.financial_class_name plan_name,
  CASE WHEN UPPER(pm.payer_group) LIKE '%MEDICAID%' THEN 'Y' ELSE 'N' END AS medicaid_ind,
  a.icd_code,
  a.problem_description,
  g.drug_name asthma_erlst_med_name,
  g.route,
  g.dosage,
  g.frequency,
  g.asthma_earlst_rx_dt,
  g.sum_days_covered_in_yr,
  g.treatment_days_in_year,
  TO_CHAR(
   CASE
    WHEN g.treatment_days_in_year = 0 THEN
     0
    WHEN g.treatment_days_in_year < g.sum_days_covered_in_yr THEN
     1.00
    WHEN g.treatment_days_in_year > g.sum_days_covered_in_yr THEN
     ROUND((g.sum_days_covered_in_yr / g.treatment_days_in_year), 2)
   END,
   9.99)
   proportion_days_covered,
  CASE
   WHEN (CASE
          WHEN g.treatment_days_in_year = 0 THEN
           0
          WHEN g.treatment_days_in_year < g.sum_days_covered_in_yr THEN
           1.00
          WHEN g.treatment_days_in_year > g.sum_days_covered_in_yr THEN
           ROUND((g.sum_days_covered_in_yr / g.treatment_days_in_year), 2)
         END) >= 0.75 THEN
    'Y'
   ELSE
    'N'
  END
   AS numeraor_flag_75_med_ratio,
  CASE
   WHEN (CASE
          WHEN g.treatment_days_in_year = 0 THEN
           0
          WHEN g.treatment_days_in_year < g.sum_days_covered_in_yr THEN
           1.00
          WHEN g.treatment_days_in_year > g.sum_days_covered_in_yr THEN
           ROUND((g.sum_days_covered_in_yr / g.treatment_days_in_year), 2)
         END) >= 0.50 THEN
    'Y'
   ELSE
    'N'
  END
   AS numeraor_flag_50_med_ratio
 FROM
  asthma_ptnt_lkp a
  LEFT JOIN pcp_vst_dt f ON f.patient_id = a.patient_id AND f.network = a.network AND pcp_visit_dt_rnum = 1
  JOIN ptnt_vst_type_cnt b ON b.patient_id = a.patient_id AND b.network = a.network
  JOIN med_luk_antbdy_dspns_evnt c ON c.patient_id = a.patient_id AND c.network = a.network
  JOIN asthma_cntrlr_med_dspns_evnt e
   ON e.patient_id = a.patient_id AND e.network = a.network AND e.ltst_cntrlr_med_rnum = 1
  JOIN asthma_other_med_dspns_evnt d
   ON d.patient_id = a.patient_id AND d.network = a.network AND d.ltst_other_med_rnum = 1
  JOIN dim_patients ptnt
   ON ptnt.patient_id = a.patient_id
      AND ptnt.network = a.network
      AND ptnt.current_flag = 1
      AND ptnt.date_of_death IS NULL
      AND (FLOOR((ADD_MONTHS(TRUNC(SYSDATE, 'year'), 12) - 1 - ptnt.birthdate) / 365) BETWEEN 5 AND 64)
  LEFT JOIN dim_payers pm ON pm.payer_id = a.first_payer_key AND pm.network = a.network
  LEFT JOIN medication_mngmnt g
   ON g.patient_id = a.patient_id AND g.network = a.network AND g.asthma_earlst_rx_rnum = 1
  LEFT JOIN ref_financial_class fin
   ON fin.financial_class_id = a.financial_class_id AND fin.network = a.network
  LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
   ON mdm.network = a.network
      AND TO_NUMBER(mdm.patientid) = a.patient_id
      AND mdm.epic_flag = 'N'
      AND a.facility_name = a.facility_name
 WHERE
  a.ptnt_prb_rnum = 1
  AND (emergency_visit_count > 0
       OR inpatient_visit_count > 0
       OR ((outpatient_visit_count >= 4 OR clinic_visit_count >= 4)
           AND (e.asthma_cntrlr_med_cnt + d.asthma_other_med_cnt) >= 2)
       OR (e.asthma_cntrlr_med_cnt + d.asthma_other_med_cnt) >= 4
       OR med_luk_antbdy_dspns_evnt_cnt >= 4);

GRANT SELECT ON v_dsrip_report_tr13_14 TO PUBLIC;