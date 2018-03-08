alter session enable  parallel DML;

SELECT   --+ noparallel
 /*+ parallel(32) */
    network,
    visit_id,
    patient_id,
    admission_date_time,
    discharge_date_time,
    facility_id,
    a1c_final_orig_value,
    a1c_final_calc_value,
    bp_final_orig_value,
    bp_final_calc_value,
    glucose_final_orig_value,
    glucose_final_calc_value,
    ldl_final_orig_value,
    ldl_final_calc_value
FROM
(
  SELECT
    q.network,
    q.visit_id,
    q.patient_id,
    q.admission_date_time,
    q.discharge_date_time,
    q.facility_id,
    q.criterion_id,
    q.value,
    REGEXP_SUBSTR(q.VALUE, '^[0-9\.<>%]*') AS extr_value
      --	 str_to_number(REGEXP_SUBSTR(q.VALUE, '^[0-9\.]+')) num_value
	FROM
  (
	  SELECT --+ ordered  use_nl(r) index_ss(r)
		  r.network,
		  r.VISIT_ID,
		  r.EVENT_ID,
		  v.PATIENT_ID,
		  v.admission_date_time,
		  v.DISCHARGE_DATE_TIME,
		  v.FACILITY_ID,
		  mc.criterion_id,
		  r.value,
		  ROW_NUMBER() OVER(PARTITION BY r.network, r.visit_id, mc.criterion_id ORDER BY r.event_id DESC) rnum
		FROM meta_conditions mc
--    JOIN meta_conditions cnl ON cnl.criterion_id = 47
    JOIN result r ON r.network = mc.network AND r.data_element_id = mc.value --AND r.value NOT LIKE cnl.value
	  JOIN visit v ON v.network  = r.network AND v.visit_id  = r.visit_id AND v.admission_date_time > date '2017-09-01'
    WHERE mc.criterion_id IN (4, 10, 23, 13) AND mc.include_exclude_ind = 'I' -- A1C, LDL, Glucose,  BP
	) q
  WHERE q.rnum = 1
)
PIVOT
(
  MAX(VALUE) AS final_orig_value, MAX(extr_value) AS final_calc_value
	FOR criterion_id
	IN (4 AS a1c, 10 AS ldl, 23 AS glucose, 13 AS bp)
);
