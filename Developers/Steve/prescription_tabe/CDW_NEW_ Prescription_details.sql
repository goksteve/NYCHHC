/*New  Prescription_details */

WITH rx 
AS
(
  SELECT --+ materialize 
    --CID, 
    rx.dosage, 
    --EMP_PROVIDER_ID,
    --FACILITY_ID,
    nvl(vst.facility_key,fclty.facility_key) AS facility_key, 
    rx.frequency, 
    rx.misc_name, 
    nvl(rx.misc_name,prc.name) as derived_product_name,
    rx.network, 
    rx.order_event_id, 
    --order_provider_string, 
    rx.order_span_id, 
    rx.order_time AS order_dt, 
    rx.order_visit_id, 
    --ordering_provider_id, 
    prvdr.provider_key,
    rx.patient_id,
    vst.patient_key, 
    --prescription_status_id, 
    --prescription_type_id, 
    rx_status.prescription_status_name AS prescription_status, 
    rx_type.prescription_type_name AS prescription_type,
    rx.proc_id, 
    prc.name AS procedure_name,
    rx.rx_allow_subs, 
    rx.rx_comment, 
    rx.rx_dc_time, 
    rx.rx_dispo, 
    rx.rx_exp_date, 
    rx.rx_id, 
    rx.rx_number, 
    --rx_pool_id, 
    rx.rx_quantity, 
    rx.rx_refills
  FROM prescription rx 
  LEFT JOIN order_item ot ON ot.order_visit_id = rx.order_visit_id AND ot.network = rx.network  AND ot.order_span_id = rx.order_span_id AND ot.event_id = rx.order_event_id
  LEFT JOIN fact_visits vst ON vst.visit_id = ot.visit_id AND vst.network = ot.network
  LEFT JOIN dim_providers prvdr ON prvdr.network = rx.network AND prvdr.provider_id = rx.ordering_provider_id and prvdr.current_flag = 1
  LEFT JOIN prescription_status rx_status ON rx_status.prescription_status_id = rx.prescription_status_id
  LEFT JOIN prescription_type rx_type ON rx_type.prescription_type_id = rx.prescription_type_id
  LEFT JOIN proc prc ON prc.network = rx.network AND prc.proc_id = rx.proc_id 
  LEFT JOIN dim_hc_facilities fclty ON fclty.network = rx.network AND fclty.facility_id = rx.facility_id
  WHERE rx.order_time >= date '2018-04-01' AND rx.order_time < date '2018-05-01'
),
rx_brg as
(
  SELECT --+materialize
    DISTINCT rx.rx_id, rx.network 
  FROM rx
  WHERE rx.misc_name IS NULL
    AND (rx.procedure_name LIKE '%eRx%' OR rx.procedure_name LIKE'%NF%')
    AND rx.misc_name IS NULL
),
dervd_name as
(
  SELECT --+materialize
    DISTINCT os.network,
    os.rx_id,
    NVL(NVL(NVL(ODRXP.NF_NAME,P.PRODUCT_NAME),MD.MEDF_DRUG_NAME),'UNKNOWN') AS DERIVED_PRODUCT_NAME
  FROM order_span os                 
  JOIN order_span_state oss ON os.network = oss.network AND os.order_span_id = oss.order_span_id AND os.visit_id = oss.visit_id
  JOIN order_definition od ON od.network = oss.network AND oss.visit_id = od.visit_id AND oss.order_span_id = od.order_span_id AND oss.order_span_state_id = od.order_span_state_id
  JOIN order_def_rx_product odrxp ON odrxp.network = od.network AND od.visit_id = odrxp.visit_id AND od.order_span_id = odrxp.order_span_id AND od.order_span_state_id = odrxp.order_span_state_id 
   AND od.order_definition_id = odrxp.order_definition_id 
  LEFT JOIN product p ON p.network = odrxp.network AND p.product_id = odrxp.product_id
  LEFT JOIN medf_drug md ON md.network = odrxp.network AND md.medf_drug_id = odrxp.medf_drug_id
  LEFT JOIN rx_brg ON rx_brg.network = os.network AND rx_brg.rx_id = os.rx_id
  WHERE (odrxp.nf_name IS NOT NULL OR p.product_name IS NOT NULL OR md.medf_drug_name IS  NOT NULL)
)
SELECT    
  distinct
  r.dosage, 
  r.facility_key, 
  r.frequency, 
  r.misc_name, 
  CASE 
    WHEN r.misc_name IS NULL
    THEN dn.derived_product_name
    ELSE r.derived_product_name
  END AS derived_product_name,
  r.network, 
  r.order_event_id, 
  r.order_span_id, 
  r.order_dt, 
  r.order_visit_id, 
  r.provider_key,
  r.patient_id,
  r.patient_key, 
  r.prescription_status, 
  r.prescription_type,
  r.proc_id, 
  r.procedure_name,
  r.rx_allow_subs, 
  r.rx_comment, 
  r.rx_dc_time, 
  r.rx_dispo, 
  r.rx_exp_date, 
  r.rx_id, 
  r.rx_number, 
  r.rx_quantity, 
  r.rx_refills
FROM rx r
LEFT JOIN dervd_name dn 
  ON dn.network = r.network AND dn.rx_id = r.rx_id;


From: Kolluru, Ganesh 
Sent: Thursday, June 07, 2018 3:10 PM
To: Shurashetty, Santosh <Santosh.Shurashetty@nychhc.org>; Umakanth Sathyanarayana (Umakanth.Sathyanarayana@nychhc.org) <Umakanth.Sathyanarayana@nychhc.org>
Cc: Steve Gorelik (goreliks1@nychhc.org) <goreliks1@nychhc.org>
Subject: Prescription_details redisigned to include visit_id

All,

Only FYI…


CREATE TABLE tst_gk_fact_rx_dtl_uma
NOLOGGING
PARALLEL 32
PARTITION BY LIST (network)
SUBPARTITION BY HASH (rx_id)
SUBPARTITIONS 16
(PARTITION cbn VALUES ('CBN'),
  PARTITION gp1 VALUES ('GP1'),
  PARTITION gp2 VALUES ('GP2'),
  PARTITION nbn VALUES ('NBN'),
  PARTITION nbx VALUES ('NBX'),
  PARTITION qhn VALUES ('QHN'),
  PARTITION sbn VALUES ('SBN'),
  PARTITION smn VALUES ('SMN'))
AS
WITH rx 
AS
(
  SELECT --+ materialize
    --CID, 
    rx.dosage, 
    --EMP_PROVIDER_ID,
    --FACILITY_ID,
    nvl(vst.facility_key,fclty.facility_key) AS facility_key, 
    rx.frequency, 
    rx.misc_name, 
    nvl(rx.misc_name,prc.name) as derived_product_name,
    rx.network, 
    rx.order_event_id, 
    --order_provider_string, 
    rx.order_span_id, 
    rx.order_time AS order_dt, 
    rx.order_visit_id, 
    --ordering_provider_id, 
    prvdr.provider_key,
    rx.patient_id,
    vst.patient_key, 
    --prescription_status_id, 
    --prescription_type_id, 
    rx_status.prescription_status_name AS prescription_status, 
    rx_type.prescription_type_name AS prescription_type,
    rx.proc_id, 
    prc.name AS procedure_name,
    rx.rx_allow_subs, 
    rx.rx_comment, 
    rx.rx_dc_time, 
    rx.rx_dispo, 
    rx.rx_exp_date, 
    rx.rx_id, 
    rx.rx_number, 
    --rx_pool_id, 
    rx.rx_quantity, 
    rx.rx_refills
  FROM prescription rx 
  LEFT JOIN order_item ot ON ot.order_visit_id = rx.order_visit_id AND ot.network = rx.network 
  LEFT JOIN fact_visits vst ON vst.visit_id = ot.visit_id AND vst.network = ot.network
  LEFT JOIN dim_providers prvdr ON prvdr.network = rx.network AND prvdr.provider_id = rx.ordering_provider_id
  LEFT JOIN prescription_status rx_status ON rx_status.prescription_status_id = rx.prescription_status_id
  LEFT JOIN prescription_type rx_type ON rx_type.prescription_type_id = rx.prescription_type_id
  LEFT JOIN proc prc ON prc.network = rx.network AND prc.proc_id = rx.proc_id 
  LEFT JOIN dim_hc_facilities fclty ON fclty.network = rx.network AND fclty.facility_id = rx.facility_id 
),
rx_brg as
(
  SELECT --+materialize
    DISTINCT rx.rx_id, rx.network 
  FROM rx
  WHERE rx.misc_name IS NULL
    AND (rx.procedure_name LIKE '%eRx%' OR rx.procedure_name LIKE'%NF%')
    AND rx.misc_name IS NULL
),
dervd_name as
(
  SELECT --+materialize
    DISTINCT os.network,
    os.rx_id,
    NVL(NVL(NVL(ODRXP.NF_NAME,P.PRODUCT_NAME),MD.MEDF_DRUG_NAME),'UNKNOWN') AS DERIVED_PRODUCT_NAME
  FROM order_span os                 
  JOIN order_span_state oss ON os.network = oss.network AND os.order_span_id = oss.order_span_id AND os.visit_id = oss.visit_id
  JOIN order_definition od ON od.network = oss.network AND oss.visit_id = od.visit_id AND oss.order_span_id = od.order_span_id AND oss.order_span_state_id = od.order_span_state_id
  JOIN order_def_rx_product odrxp ON odrxp.network = od.network AND od.visit_id = odrxp.visit_id AND od.order_span_id = odrxp.order_span_id AND od.order_span_state_id = odrxp.order_span_state_id 
   AND od.order_definition_id = odrxp.order_definition_id 
  LEFT JOIN product p ON p.network = odrxp.network AND p.product_id = odrxp.product_id
  LEFT JOIN medf_drug md ON md.network = odrxp.network AND md.medf_drug_id = odrxp.medf_drug_id
  LEFT JOIN rx_brg ON rx_brg.network = os.network AND rx_brg.rx_id = os.rx_id
  WHERE (odrxp.nf_name IS NOT NULL OR p.product_name IS NOT NULL OR md.medf_drug_name IS  NOT NULL)
)
SELECT    
  r.dosage, 
  r.facility_key, 
  r.frequency, 
  r.misc_name, 
  CASE 
    WHEN r.misc_name IS NULL
    THEN dn.derived_product_name
    ELSE r.derived_product_name
  END AS derived_product_name,
  r.network, 
  r.order_event_id, 
  r.order_span_id, 
  r.order_dt, 
  r.order_visit_id, 
  r.provider_key,
  r.patient_id,
  r.patient_key, 
  r.prescription_status, 
  r.prescription_type,
  r.proc_id, 
  r.procedure_name,
  r.rx_allow_subs, 
  r.rx_comment, 
  r.rx_dc_time, 
  r.rx_dispo, 
  r.rx_exp_date, 
  r.rx_id, 
  r.rx_number, 
  r.rx_quantity, 
  r.rx_refills
FROM rx r
LEFT JOIN dervd_name dn 
  ON dn.network = r.network AND dn.rx_id = r.rx_id;

