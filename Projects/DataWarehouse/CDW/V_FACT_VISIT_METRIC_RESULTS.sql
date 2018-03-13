CREATE OR REPLACE VIEW V_FACT_VISIT_METRIC_RESULTS 
AS
SELECT 
 network,
 visit_id,
 a1c_final_orig_value,
 a1c_final_calc_value,
 ldl_final_orig_value,
 ldl_final_calc_value,
 glucose_final_orig_value,
 glucose_final_calc_value,
 bp_final_orig_value,
 bp_final_calc_value
FROM
 ( 

SELECT
	 q.*, --REGEXP_SUBSTR(q.VALUE, '^[0-9\. < > %]*') AS extr_value
			 str_to_number(REGEXP_SUBSTR(q.result_VALUE, '^[0-9\.]+')) num_value
FROM(
WITH 
crit_rem
AS
(
  SELECT
  value
  FROM
  meta_conditions
  WHERE
  criterion_id = 47
),
crit_wc
AS
(
  SELECT
  network,
  criterion_id,
  value
  FROM
  meta_conditions
  WHERE
  criterion_id IN (4, 10,23,13)
  AND include_exclude_ind = 'I'
) -- A1C, LDL, Glucose,  BP 

SELECT
 r.network,
 r.visit_id,
 r.result_value,
 c.criterion_id,
 ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, c.criterion_id ORDER BY r.event_id DESC) rnum
FROM
 crit_wc c
 JOIN fact_results r ON r.data_element_id = c.VALUE AND r.network = c.network
 JOIN crit_rem t ON r.result_value NOT LIKE t.value)q
	WHERE 	 q.rnum = 1

	 )
 PIVOT
	(MAX(Result_VALUE) AS final_orig_value, MAX(num_value) AS final_calc_value
	FOR criterion_id
	IN (4 AS a1c, 10 AS ldl, 23 AS glucose, 13 AS bp))
	