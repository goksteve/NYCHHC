CREATE OR REPLACE VIEW v_fact_visits_incremental AS
WITH
 -- 14-MAR-2018, OK: added incremental logic
  visit_info AS
  (
    SELECT --+ ordered use_nl(v vs vl) materialize
      v.network, 
      v.visit_id, 
      v.visit_number, 
      v.patient_id, 
      v.facility_id,
      v.attending_emp_provider_id,
      v.resident_emp_provider_id,
      vs.admitting_emp_provider_id,
      vs.emp_provider_id, 
      vs.visit_type_id initial_visit_type_id,
      v.visit_type_id, 
      v.visit_status_id,
      vl.location_id, 
      v.admission_date_time,
      v.discharge_date_time,
      vs.activation_time,
      v.physician_service_id,
      v.financial_class_id,
      LAST_VALUE(vl.location_id) OVER
      (
        PARTITION BY v.network, v.visit_id ORDER BY vl.visit_segment_number DESC, vl.location_id DESC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) last_loc_id,
      ROW_NUMBER() OVER(PARTITION BY v.network, v.visit_id ORDER BY vl.visit_segment_number, vl.location_id) loc_rnum,
      v.cid
    FROM log_incremental_data_load delta
    JOIN visit v
      ON v.network = delta.network AND v.cid > delta.max_cid
    LEFT JOIN visit_segment vs
      ON vs.network = v.network AND vs.visit_id = v.visit_id AND vs.visit_segment_number = 1
    LEFT JOIN visit_segment_visit_location vl
      ON vl.network = v.network AND vl.visit_id = v.visit_id
    WHERE delta.table_name = 'FACT_VISITS'
  )
SELECT --_ ordered use_nl(p f d1 d2 pr1 pr2 pr3 pr4 vsp dp vsn)
  vi.network,
  vi.visit_id,
  p.patient_key,
  p.patient_id,
  f.facility_key,
  d1.department_key first_department_key,
  d2.department_key last_department_key,
  pr1.provider_key attending_provider_key,
  pr2.provider_key resident_provider_key,
  pr3.provider_key admitting_provider_key,
  pr4.provider_key visit_emp_provider_key,
  vi.admission_date_time AS admission_dt,
  TO_NUMBER(TO_CHAR(vi.admission_date_time, 'YYYYMMDD')) AS admission_dtnum,
  FLOOR((ADD_MONTHS(TRUNC(vi.admission_date_time,'YEAR'),12)-1 - p.birthdate)/365)  patient_age_at_admission,
  vi.discharge_date_time discharge_dt,
  nvl(vi.visit_number, vsn.visit_secondary_number) AS visit_number, 
  nvl(vi.initial_visit_type_id, vi.visit_type_id) AS initial_visit_type_id,
  vi.visit_type_id final_visit_type_id,
  vi.visit_status_id,
  vi.activation_time visit_activation_time,
  vi.financial_class_id,
  vi.physician_service_id,
  dp.payer_key first_payer_key,
  'QCPR' source,
  vi.cid
FROM visit_info vi
JOIN dim_patients p
  ON p.network = vi.network AND p.patient_id = vi.patient_id AND p.current_flag=1
JOIN dim_hc_facilities f
  ON f.network = vi.network AND f.facility_id = vi.facility_id
LEFT JOIN dim_hc_departments d1
  ON d1.network = vi.network AND d1.location_id = vi.location_id 
LEFT JOIN dim_hc_departments d2
  ON d2.network = vi.network AND d2.location_id = vi.last_loc_id
LEFT JOIN dim_providers pr1
  ON pr1.network = vi.network AND pr1.provider_id = vi.attending_emp_provider_id AND pr1.current_flag = 1
LEFT JOIN dim_providers pr2 
  ON pr2.network = vi.network AND pr2.provider_id = vi.resident_emp_provider_id AND pr2.current_flag = 1
LEFT JOIN dim_providers pr3
  ON pr3.network = vi.network AND pr3.provider_id = vi.admitting_emp_provider_id AND pr3.current_flag = 1 
LEFT JOIN dim_providers pr4
  ON pr4.network = vi.network AND pr4.provider_id = vi.emp_provider_id AND pr4.current_flag = 1
LEFT JOIN visit_segment_payer vsp 
  ON vsp.visit_id = vi.visit_id AND vsp.network=vi.network
 AND vsp.visit_segment_number = 1 AND vsp.payer_number = 1
LEFT JOIN dim_payers dp 
  ON dp.payer_id = vsp.payer_id AND dp.network = vsp.network
LEFT JOIN map_visit_sec_nbr_type mpt
  ON mpt.network = vi.network AND mpt.facility_id = vi.facility_id
LEFT JOIN visit_secondary_number vsn
  ON vsn.visit_id = vi.visit_id
 AND vsn.network = vi.network
 AND vsn.visit_sec_nbr_type_id = mpt.visit_sec_nbr_type_id
 AND vsn.visit_sec_nbr_nbr = DECODE(vi.network, 'GP1', 1, 'GP2', 1, vsn.visit_sec_nbr_nbr)
WHERE vi.loc_rnum = 1;