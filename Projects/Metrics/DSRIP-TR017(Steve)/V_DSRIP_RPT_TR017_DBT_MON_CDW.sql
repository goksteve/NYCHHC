CREATE OR REPLACE VIEW v_dsrip_rpt_tr017_dbt_mon_cdw AS
 WITH tmp AS
       (

SELECT
         dsrip_report,
         report_dt,
         network,
         patient_id,
         facility_id,
         facility_name,
         pat_lname,
         pat_fname,
         mrn,
         birthdate,
         age,
         pcp,
         last_pcp_visit_dt,
         visit_type,
         admission_dt AS latest_admission_dt,
         discharge_dt,
         last_bh_facility,
         last_bh_visit_dt,
         last_bh_provider,
         medicaid_ind,
         payer_group,
         payer_id,
         payer_name,
         plan_name,
         comb_ind,
         a1c_ind,
         ldl_ind,
         ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY admission_dt DESC) cnt
        FROM
         dsrip_tr017_diab_mon_cdw
        WHERE
         report_dt = (SELECT MAX(report_dt) FROM dsrip_tr017_diab_mon_cdw)


)
 SELECT
  p.dsrip_report,
  p.report_dt,
  p.network,
  p.patient_id,
  p.facility_id,
  p.facility_name,
  p.pat_lname,
  p.pat_fname,
  p.mrn,
  p.birthdate,
  p.age,
  p.pcp,
  p.last_pcp_visit_dt,
  p.visit_type,
  p.latest_admission_dt,
  p.discharge_dt,
  p.last_bh_facility,
  p.last_bh_visit_dt,
  p.last_bh_provider,
  p.medicaid_ind,
  p.payer_group,
  p.payer_id,
  p.payer_name,
  p.plan_name,
  p.comb_ind,
  p.a1c_ind,
  p.ldl_ind,
  a1c_calc_value,
  ldl_calc_value
 FROM
  tmp p
  JOIN (
        SELECT
         *
        FROM
         (
          SELECT
           network,
           patient_id,
           calc_result_value,
           test_type
          FROM
           dsrip_tr017_diab_mon_cdw
          WHERE
           report_dt = (SELECT MAX(report_dt) FROM dsrip_tr017_diab_mon_cdw)
         )
         PIVOT
          (MAX(calc_result_value) AS calc_value
          FOR test_type
          IN ('A1C' AS a1c, 'LDL' AS ldl))
       ) r
   ON r.network = p.network AND r.patient_id = p.patient_id
 WHERE
  p.cnt = 1

