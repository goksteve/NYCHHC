CREATE OR REPLACE VIEW V_FACT_VISIT_PAYERS
 AS
WITH tmp_payer AS
 (
   SELECT --+ materialize 
  * FROM
(
  SELECT
 p.network, v.visit_key, p.visit_id, payer_number, p.payer_id, pp.payer_key
FROM
 visit_segment_payer p
 JOIN fact_visits v ON v.network = p.network AND v.visit_id = p.visit_id
 JOIN dim_payers pp ON pp.network = p.network AND pp.payer_id = p.payer_id
WHERE
 p.payer_id IS NOT NULL AND p.payer_number < 5
 )
PIVOT
(   MAX(payer_id )   AS payer_id,
    MAX(payer_key ) AS payer_key
    FOR payer_number IN (1 AS FIRST, 2 AS SECOND, 3 AS third , 4 AS fourth)
)
)
SELECT
 network visit_key,
 visit_id,
 first_payer_id,
 first_payer_key,
 second_payer_id,
 second_payer_key,
 third_payer_id,
 third_payer_key,
 fourth_payer_id,
 fourth_payer_key
FROM
 tmp_payer;