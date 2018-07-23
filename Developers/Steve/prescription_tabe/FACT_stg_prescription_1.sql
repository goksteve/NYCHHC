create OR REPLACE VIEW V_FACT_PATIENT_PRESCRIP_DET
 AS

WITH rx AS
   (
    SELECT --+ materialize
        rx.network,
        vst.patient_key,
        fclty.facility_id,
        fclty.facility_name,
        TO_NUMBER(TO_CHAR(NVL(rx.order_time, DATE '2010-01-01'), 'YYYYMMDD')) AS datenum_order_dt_key,
        prvdr.provider_id,
        rx.patient_id,
        rx.order_visit_id,
        rx.order_event_id,
        rx.order_span_id,
        NVL(TRUNC(rx.order_time), DATE '2010-01-01') AS order_dt,
        NVL(LOWER(rx.misc_name), 'n/a') AS drug_description,
        rx.dosage,
        rx.frequency,
        rx.rx_quantity,
        rx.rx_refills,
        rx.misc_name,
        rx_status.prescription_status_name AS prescription_status,
        rx_type.prescription_type_name AS prescription_type,
        rx.proc_id,
        NVL(LOWER(prc.name), 'n/a') AS drug_name,
        rx.rx_allow_subs,
        rx.rx_comment,
        TRUNC(rx.rx_dc_time) AS rx_dc_time,
        rx.rx_dispo,
        TRUNC(rx.rx_exp_date) AS rx_exp_date,
        rx.rx_id,
        rx.rx_number,
        NVL(rx.misc_name, prc.name) AS derived_product_name
       FROM
        prescription rx
        LEFT JOIN order_item ot ON ot.order_visit_id = rx.order_visit_id AND ot.network = rx.network AND ot.order_span_id = rx.order_span_id AND ot.event_id = rx.order_event_id
        LEFT JOIN fact_visits vst ON vst.visit_id = ot.visit_id AND vst.network = ot.network
        LEFT JOIN dim_providers prvdr ON prvdr.network = rx.network AND prvdr.provider_id = rx.ordering_provider_id AND prvdr.current_flag = 1
        LEFT JOIN prescription_status rx_status ON rx_status.network = rx.network AND rx_status.prescription_status_id = rx.prescription_status_id
        LEFT JOIN prescription_type rx_type ON rx_type.network = rx.network AND rx_type.prescription_type_id = rx.prescription_type_id
        LEFT JOIN proc prc ON prc.network = rx.network AND prc.proc_id = rx.proc_id
        LEFT JOIN dim_hc_facilities fclty ON fclty.network = rx.network AND fclty.facility_id = rx.facility_id
     ),
  rx_brg AS
    (
      SELECT --+ materialize
      DISTINCT rx.rx_id, rx.network
      FROM
      rx
      WHERE
      rx.misc_name IS NULL
      AND (UPPER(rx.drug_name) LIKE '%ERX%' OR UPPER(rx.drug_name) LIKE '%NF%')
      AND rx.misc_name IS NULL
  ),
     dervd_name AS
   (
     SELECT --+materialize
      DISTINCT
      os.network,
      os.rx_id,
      NVL(NVL(NVL(odrxp.nf_name, p.product_name), md.medf_drug_name), 'UNKNOWN') AS derived_product_name
    FROM
      order_span os
      JOIN order_span_state oss ON os.network = oss.network AND os.order_span_id = oss.order_span_id AND os.visit_id = oss.visit_id
      JOIN order_definition od ON od.network = oss.network  AND oss.visit_id = od.visit_id AND oss.order_span_id = od.order_span_id AND oss.order_span_state_id = od.order_span_state_id 
      JOIN order_def_rx_product odrxp ON odrxp.network = od.network AND od.visit_id = odrxp.visit_id AND od.order_span_id = odrxp.order_span_id 
                                        AND od.order_span_state_id = odrxp.order_span_state_id AND od.order_definition_id = odrxp.order_definition_id
      LEFT JOIN product p ON p.network = odrxp.network AND p.product_id = odrxp.product_id
      LEFT JOIN medf_drug md ON md.network = odrxp.network AND md.medf_drug_id = odrxp.medf_drug_id
      LEFT JOIN rx_brg ON rx_brg.network = os.network AND rx_brg.rx_id = os.rx_id
  WHERE
     odrxp.nf_name IS NOT NULL 
     OR p.product_name IS NOT NULL 
     OR md.medf_drug_name IS NOT NULL
 )
    SELECT /*+ PARALLEl (32) */
     rx.network,
     rx.patient_key,
     rx.facility_id,
     rx.facility_name,
     rx.datenum_order_dt_key,
     rx.provider_id,
     CAST(rx.patient_id AS VARCHAR2(256)) patient_id,
     rx.order_visit_id,
     rx.order_event_id,
     rx.order_span_id,
     rx.order_dt,
     rx.drug_description,
     rx.dosage,
     rx.frequency,
     rx.rx_quantity,
     rx.rx_refills,
     rx.misc_name,
     rx.prescription_status,
     rx.prescription_type,
     rx.proc_id,
     rx.drug_name,
     rx.rx_allow_subs,
     rx.rx_comment,
     rx.rx_dc_time,
     rx.rx_dispo,
     rx.rx_exp_date,
     rx.rx_id,
     rx.rx_number,
     DATE '1900-01-01' AS start_date,
     DATE '1900-01-01' AS stop_date,
     DATE '1900-01-01' AS archive_date_time,
     CASE WHEN rx.misc_name IS NULL THEN dn.derived_product_name ELSE rx.derived_product_name END
      AS derived_product_name,
     'QCPR' AS source,
     TRUNC(SYSDATE) AS load_dt
    FROM
     rx LEFT JOIN dervd_name dn ON dn.network = rx.network AND dn.rx_id = rx.rx_id
UNION
      SELECT
       NVL(x.network, 'EPIC') AS network,
       999999 AS patient_key,
       x.facility_id,
       x.facility_name,
       TO_NUMBER(TO_CHAR(a.ordering_date, 'YYYYMMDD')) AS datenum_order_dt_key,
       TO_NUMBER(NVL(REGEXP_REPLACE(a.ord_prov_id, '[^0-9]'), 999999999999)) AS ord_prov_id,
       a.pat_id AS patient_id,
       TO_NUMBER(REGEXP_REPLACE(a.pat_enc_csn_id, '[^[:digit:]]')) AS order_visit_id,
       CASE
        WHEN CAST(a.ordering_date AS DATE) IS NOT NULL THEN
         TO_NUMBER(TO_CHAR(CAST(a.ordering_date AS DATE), 'YYYYMMDD'))
        ELSE
         NULL
       END
        AS order_event_id,
       999999 AS order_span_id,
       CAST(a.ordering_date AS DATE) AS opder_dt,
       a.description AS drug_description,
       a.dosage AS dosage,
       a.sig AS frequency,
       TO_NUMBER(REGEXP_REPLACE(a.quantity, '[^[:digit:]]')) AS quantity,
       TO_NUMBER(REGEXP_REPLACE(a.refills, '[^[:digit:]]')) AS refills,
       g.name AS misc_name,
       o.name AS prescription_status,
       '-99' AS prescription_type,
       u.med_linked_proc_id AS proc_id,
       v.proc_name AS drug_name,
       '-99' AS rx_allow_subs,
       a.med_comments AS rx_comment,
       CAST(a.discon_time AS DATE) AS rx_dc_time,
       '-99' AS rx_dispo,
       CAST(z.prescrip_exp_date AS DATE) AS rx_exp_date,
       TO_NUMBER(REGEXP_REPLACE(a.order_med_id, '[^[:digit:]]')) AS rx_id,
       '-99' AS rx_number,
       CAST(a.start_date AS DATE) AS start_date,
       CAST(a.end_date AS DATE) AS end_date,
       DATE '2099-01-01' AS archive_date_time,
       NVL(g.name, a.display_name) AS derived_product_name,
       'EPIC' AS source,
       TRUNC(SYSDATE) AS load_dt
      FROM
       epic_clarity.order_med a
       LEFT OUTER JOIN epic_clarity.patient_3 b ON b.pat_id = a.pat_id
       LEFT OUTER JOIN epic_clarity.clarity_dep c ON c.department_id = a.pat_loc_id
       LEFT OUTER JOIN epic_clarity.clarity_loc d ON d.loc_id = c.rev_loc_id
       LEFT OUTER JOIN epic_clarity.x_loc_facility_mapping x ON x.facility_id = d.adt_parent_id
       LEFT OUTER JOIN epic_clarity.clarity_medication g ON a.medication_id = g.medication_id
       LEFT OUTER JOIN epic_clarity.zc_order_status o ON o.order_status_c = a.order_status_c
       LEFT OUTER JOIN epic_clarity.order_medinfo u ON u.order_med_id = a.order_med_id
       LEFT OUTER JOIN epic_clarity.clarity_eap v ON v.proc_id = u.med_linked_proc_id
       LEFT OUTER JOIN epic_clarity.order_med_3 z ON z.order_id = a.order_med_id
      WHERE
       (b.is_test_pat_yn <> 'Y')
       AND a.ordering_mode_c != 2 -- REMOVING INPATIENTS
       AND CAST(a.ordering_date AS DATE) BETWEEN '01-APR-2016' AND TRUNC(SYSDATE, 'MM') -- FOR APPEND DATA NEXT MONTH
       AND a.pend_action_c IN (2,6,1,5,7) --Reorder,Reorder from Order Review,Change,Reorder from Medication Activity,Reorder from Reports