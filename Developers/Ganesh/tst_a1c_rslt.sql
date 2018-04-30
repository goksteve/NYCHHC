--select --+ parallel(32) 
--a.p_systolic,a.p_diastolic,a.p_result_dt,m.* 
--from fact_visits m, table(tst_gk_ltst_bp_val(m.network,m.visit_id)) a
--where m.network='GP1' and m.visit_id=26659651;
--           

select --+ parallel(32) 
a.p_a1c_rslt, a.p_diabetes_diag_flag, a.p_a1c_test_flag, a.p_a1c_rslt_dt,
m.* 
from fact_visits m, table(tst_gk_ltst_a1c_val(m.network,m.visit_id)) a
where m.network='CBN' and m.visit_id=23890427;


SELECT * FROM meta_conditions WHERE criterion_id = 33 and value_description='Diabetic supply';

WITH 
 dt AS 
  (
    SELECT --+ materialize
      TRUNC(SYSDATE, 'MONTH') AS report_period_start_dt,
      ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) begin_dt,
      TRUNC (ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -1), 'MONTH') end_dt
    FROM DUAL
  ),
  diab_ptnt_lkp AS 
  (
    SELECT --+ materialize
      fpd.network,
      fpd.patient_id,
      fpd.onset_date,
      fpd.diag_code htn_dx_code,
      ROW_NUMBER() OVER (PARTITION BY fpd.patient_id ORDER BY fpd.onset_date DESC) diab_ptnt_rnum   
    FROM --dt JOIN 
    fact_patient_diagnoses fpd --on  fpd.onset_date >= dt.msrmnt_yr_start_dt 
    JOIN meta_conditions mc
      ON mc.value=fpd.diag_code AND mc.criterion_id=1 AND include_exclude_ind='I' AND mc.QUALIFIER='ICD-10'
    WHERE NOT EXISTS
      (
        SELECT 
          distinct fpd1.patient_id,fpd1.network
        FROM --dt JOIN 
        fact_patient_diagnoses fpd1 --on  fpd1.onset_date >= dt.msrmnt_yr_start_dt 
        JOIN meta_conditions mc1 ON mc1.value=fpd1.diag_code AND mc1.criterion_id=1 AND mc1.include_exclude_ind='E'  AND mc1.QUALIFIER='ICD-10' 
        WHERE fpd1.patient_id=fpd.patient_id AND fpd1.network=fpd.network    
      )
  ),
  diab_tst_lkp AS
  (
    SELECT 
      DISTINCT network,a.patient_id
    FROM fact_patient_prescriptions a
    JOIN ref_drug_descriptions b
      ON a.drug_description = b.drug_description
      AND b.drug_type_id = 33
  ),
  a1c_rslt AS
  (
    SELECT 
      CASE
        WHEN SUBSTR(r.result_value, 1, 1) <> '0' AND REGEXP_COUNT(r.result_value, '\.', 1) <= 1 AND SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(r.result_value, '[^[:digit:].]'), '\.$'), 1, 38) <= 50 
        THEN SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(r.result_value, '[^[:digit:].]'), '\.$'), 1, 5)
      END a1c_rslt,
      result_dt,
      NVL2(diag_lkp.patient_id,1,0) diabetes_diag_flag,
      NVL2(tst_lkp.patient_id,1,0) diabetes_test_flag,
      row_number() over (partition by r.network,r.visit_id order by r.result_dt desc) rnum_per_visit
    FROM meta_conditions mc
    JOIN fact_results r ON r.data_element_id = mc.value AND mc.criterion_id = 4
    LEFT JOIN diab_ptnt_lkp diag_lkp ON diag_lkp.network=r.network AND diag_lkp.patient_id = r.patient_id
    LEFT JOIN diab_tst_lkp tst_lkp ON tst_lkp.network=r.network AND tst_lkp.patient_id = r.patient_id
--    WHERE r.visit_id = p_visit_id_in AND r.network = p_network_in
    WHERE r.network='CBN' and r.visit_id=23890427
  )    
select --+ parallel(32) 
a1c_rslt, diabetes_diag_flag, diabetes_test_flag, result_dt from a1c_rslt where rnum_per_visit=1;     
--p_a1c_rslt varchar2(256), p_diabetes_diag_flag varchar2(10), p_a1c_test_flag varchar2(10),p_a1c_rslt_dt date
select * from TST_UK_F_CL_VST_DIAG_CBN

WHEN q.criterion_id = 4 THEN --  A1C
   CASE
    WHEN SUBSTR(q.result_value, 1, 1) <> '0'
         AND REGEXP_COUNT(q.result_value, '\.', 1) <= 1
         AND SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$'), 1, 38) <=
              50 THEN
     SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(q.result_value, '[^[:digit:].]'), '\.$'), 1, 5)
   END;
   
   
   
   select --+ parallel(32) 
   a.network,a.visit_id,a.result_value,b.value_description,a.result_dt,
   CASE
    WHEN SUBSTR(a.result_value, 1, 1) <> '0'
         AND REGEXP_COUNT(a.result_value, '\.', 1) <= 1
         AND SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(a.result_value, '[^[:digit:].]'), '\.$'), 1, 38) <=
              50 THEN
     SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(a.result_value, '[^[:digit:].]'), '\.$'), 1, 5)
   END   
 from fact_results a join
   meta_conditions b on b.value=a.data_element_id and b.criterion_id=4 
   where a.network='SBN';
   
   select * from meta_conditions where criterion_id=33 and VALUE_DESCRIPTION='Diabetic supply'; --2,785 35criterion_id
select * from meta_conditions where criterion_id=35     
  select * from meta_criteria where criterion_id=1;
  
  select * from ref_drug_descriptions where drug_type_id=35;
  
  
  