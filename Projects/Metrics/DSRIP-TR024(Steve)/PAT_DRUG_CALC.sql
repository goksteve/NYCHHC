drop table steve_del_pat_drug;
create table steve_del_pat_drug
AS
WITH report_dates AS
   (
    SELECT --+ materialize
    -- ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1)report_dt,
    TRUNC(SYSDATE, 'MONTH') report_dt,
    ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) start_dt,
    ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -28) drug_calc_dt,
    --ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) rslt_start_date,
    ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
    FROM DUAL
   ) ,

tmp_drug_pat
AS
(
SELECT --+ materialize
 d.network,
 d.patient_id,
 d.drug_name,
 d.drug_description,
 d.dosage,
 d.rx_quantity,
 d.frequency,
 DRUG_FREQUENCY_NUM_VAL AS daily_cnt,
 d.rx_dc_dt,
 d.rx_exp_dt,
 trunc(d.order_dt) as order_dt ,
 LAG  (order_dt, 1) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS prev_order_dt,
 LEAD (order_dt, 1) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS next_order_dt,
 LEAD (order_dt, 2) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS second_next_order_dt,
 LEAD (order_dt, 3) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS third_next_order_dt,
 LEAD (order_dt, 4) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS fourth_next_order_dt,
 LEAD (order_dt, 5) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) AS fifth_next_order_dt,
 trunc(order_dt) - LAG  (order_dt, 1) OVER(PARTITION BY d.network, d.patient_id ORDER BY order_dt) as diff_days ,
 start_dt  as rep_start_dt
 --ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY order_dt) cnt
FROM
 report_dates dt
 CROSS JOIN fact_patient_prescriptions d
 JOIN ref_drug_descriptions rd  ON rd.drug_description = d.drug_description AND rd.drug_type_id = 103
 JOIN ref_drug_frequency a ON d.frequency LIKE a.drug_frequency
WHERE
 order_dt >= drug_calc_dt AND order_dt < dt.report_dt
order by d.network,d.patient_id
),
drug_pat
AS(
    
 SELECT --+ materialize
    network,
    patient_id,
    drug_name,
    drug_description,
    dosage,
    rx_quantity,
    frequency ,
   daily_cnt,
    rx_dc_dt,
    rx_exp_dt,
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
  rx_dc_dt,
  rx_exp_dt,
  order_dt,
  next_order_dt,
  second_next_order_dt,
  third_next_order_dt,
  fourth_next_order_dt,
  fifth_next_order_dt,
  diff_days,
  rep_start_dt,
  ROW_NUMBER() OVER(PARTITION BY network, patient_id ORDER BY order_dt, rx_quantity DESC ) cnt
FROM
 tmp_drug_pat
WHERE
 order_dt >= rep_start_dt
 )
where cnt  = 1 and ( diff_days > 105 or diff_days is null)
)

-- select --+ parallel(32) 
--* from drug_pat

select --+ parallel(32)
  network,
  patient_id,
  drug_name,
  drug_description,
  dosage,
  rx_quantity,
  frequency, 
  daily_cnt, 
  rx_dc_dt,
  rx_exp_dt,
  order_dt,
CASE 
   WHEN  rx_quantity/daily_cnt < 85 THEN
   CASE WHEN  next_order_dt is null then 0
        WHEN rx_quantity < 61 and  order_dt -  next_order_dt > 30 THEN 0
        WHEN rx_quantity < 31 and  next_order_dt - second_next_order_dt > 30  then 0
ELSE 1
END
ELSE 1
END as numerator_flag
FROM drug_pat