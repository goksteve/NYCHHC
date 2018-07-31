alter session enable parallel DDl;
alter session enable parallel DMl;
DROP TABLE stg_fact_patient_prescrip_2;

CREATE TABLE stg_fact_patient_prescrip_2
NOLOGGING
PARTITION BY LIST (network)
 (PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'),
  PARTITION epic VALUES ('EPIC'))

 AS
  SELECT /*+ Parallel(32) */
    DISTINCT
    NVL(x.network,'EPIC') as network,
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
    END   AS order_event_id,
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
    date '2099-01-01'AS archive_date_time,
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
