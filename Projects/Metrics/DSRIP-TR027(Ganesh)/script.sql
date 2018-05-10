select --+ parallel(32) 
network, patient_id, count(visit_id) from fact_visit_diagnoses diag
JOIN 
table(tab_v256('F10.10', 'F10.120', 'F10.121', 'F10.129', 'F10.14', 'F10.150', 'F10.151', 'F10.159', 'F10.180', 'F10.181', 'F10.182', 'F10.188', 'F10.19', 'F10.20', 'F10.220', 'F10.221', 'F10.229', 'F10.230', 'F10.231', 'F10.232', 'F10.239', 'F10.24', 'F10.250', 'F10.251', 'F10.259', 'F10.26', 'F10.27', 'F10.280', 'F10.281', 'F10.282', 'F10.288', 'F10.29', 'F11.10', 'F11.120', 'F11.121', 'F11.122', 'F11.129', 'F11.14', 'F11.150', 'F11.151', 'F11.159', 'F11.181', 'F11.182', 'F11.188', 'F11.19', 'F11.20', 'F11.220', 'F11.221', 'F11.222', 'F11.229', 'F11.23', 'F11.24', 'F11.250', 'F11.251', 'F11.259', 'F11.281', 'F11.282', 'F11.288', 'F11.29', 'F12.10', 'F12.120', 'F12.121', 'F12.122', 'F12.129', 'F12.150', 'F12.151', 'F12.159', 'F12.180', 'F12.188', 'F12.19', 'F12.20', 'F12.220', 'F12.221', 'F12.222', 'F12.229', 'F12.250', 'F12.251', 'F12.259', 'F12.280', 'F12.288', 'F12.29', 'F13.10', 'F13.120', 'F13.121', 'F13.129', 'F13.14', 'F13.150', 'F13.151', 'F13.159', 'F13.180', 'F13.181', 'F13.182', 'F13.188', 'F13.19', 'F13.20', 'F13.220', 'F13.221', 'F13.229', 'F13.230', 'F13.231', 'F13.232', 'F13.239', 'F13.24', 'F13.250', 'F13.251', 'F13.259', 'F13.26', 'F13.27', 'F13.280', 'F13.281', 'F13.282', 'F13.288', 'F13.29', 'F14.10', 'F14.120', 'F14.121', 'F14.122', 'F14.129', 'F14.14', 'F14.150', 'F14.151', 'F14.159', 'F14.180', 'F14.181', 'F14.182', 'F14.188', 'F14.19', 'F14.20', 'F14.220', 'F14.221', 'F14.222', 'F14.229', 'F14.23', 'F14.24', 'F14.250', 'F14.251', 'F14.259', 'F14.280', 'F14.281', 'F14.282', 'F14.288', 'F14.29', 'F15.10', 'F15.120', 'F15.121', 'F15.122', 'F15.129', 'F15.14', 'F15.150', 'F15.151', 'F15.159', 'F15.180', 'F15.181', 'F15.182', 'F15.188', 'F15.19', 'F15.20', 'F15.220', 'F15.221', 'F15.222', 'F15.229', 'F15.23', 'F15.24', 'F15.250', 'F15.251', 'F15.259', 'F15.280', 'F15.281', 'F15.282', 'F15.288', 'F15.29', 'F16.10', 'F16.120', 'F16.121', 'F16.122', 'F16.129', 'F16.14', 'F16.150', 'F16.151', 'F16.159', 'F16.180', 'F16.183', 'F16.188', 'F16.19', 'F16.20', 'F16.220', 'F16.221', 'F16.229', 'F16.24', 'F16.250', 'F16.251', 'F16.259', 'F16.280', 'F16.283', 'F16.288', 'F16.29', 'F18.10', 'F18.120', 'F18.121', 'F18.129', 'F18.14', 'F18.150', 'F18.151', 'F18.159', 'F18.17', 'F18.180', 'F18.188', 'F18.19', 'F18.20', 'F18.220', 'F18.221', 'F18.229', 'F18.24', 'F18.250', 'F18.251', 'F18.259', 'F18.27', 'F18.280', 'F18.288', 'F18.29', 'F19.10', 'F19.120', 'F19.121', 'F19.122', 'F19.129', 'F19.14', 'F19.150', 'F19.151', 'F19.159', 'F19.16', 'F19.17', 'F19.180', 'F19.181', 'F19.182', 'F19.188', 'F19.19', 'F19.20', 'F19.220', 'F19.221', 'F19.222', 'F19.229', 'F19.230', 'F19.231', 'F19.232', 'F19.239', 'F19.24', 'F19.250', 'F19.251', 'F19.259', 'F19.26', 'F19.27', 'F19.280', 'F19.281', 'F19.282', 'F19.288', 'F19.29')) aod_icd
ON aod_icd.column_value = diag.icd_code
JOIN DIM_DATE_TIME dt
ON dt.datenum = diagnosis_dt_key AND dt.year = 2017
group by network, patient_id
having count(visit_id) > 5;

CBN	119749	121
CBN	1843567	15
GP1	1222734	18
GP1	151345	16
GP2	477559	25
GP2	189047	15

select * from REF_VISIT_TYPES;
1	Inpatient
2	Emergency
3	Outpatient
4	Clinic
5	Referral
6	Ambulatory Surgery
7	Historical
8	Lifecare Visit
9	Home Health


WITH tst 
AS
(
  SELECT --+ parallel(32) materialize
--lag(v.admission_dt, 1) OVER (partition by diag.network, diag.patient_id order by v.admission_dt) prev_visit, v.admission_dt, diag.* 
    diag.network, diag.visit_id, v.admission_dt, diag.icd_code, diag.coding_scheme, diag.patient_id, diag.facility_key, diag.problem_comments, p.birthdate,
    row_number() over (partition by diag.network, diag.visit_id order by null) diag_rnum
  FROM fact_visit_diagnoses diag
  JOIN 
  table(tab_v256('F10.10', 'F10.120', 'F10.121', 'F10.129', 'F10.14', 'F10.150', 'F10.151', 'F10.159', 'F10.180', 'F10.181', 'F10.182', 'F10.188', 'F10.19', 'F10.20', 'F10.220', 'F10.221', 'F10.229', 'F10.230', 'F10.231', 'F10.232', 'F10.239', 'F10.24', 'F10.250', 'F10.251', 'F10.259', 'F10.26', 'F10.27', 'F10.280', 'F10.281', 'F10.282', 'F10.288', 'F10.29', 'F11.10', 'F11.120', 'F11.121', 'F11.122', 'F11.129', 'F11.14', 'F11.150', 'F11.151', 'F11.159', 'F11.181', 'F11.182', 'F11.188', 'F11.19', 'F11.20', 'F11.220', 'F11.221', 'F11.222', 'F11.229', 'F11.23', 'F11.24', 'F11.250', 'F11.251', 'F11.259', 'F11.281', 'F11.282', 'F11.288', 'F11.29', 'F12.10', 'F12.120', 'F12.121', 'F12.122', 'F12.129', 'F12.150', 'F12.151', 'F12.159', 'F12.180', 'F12.188', 'F12.19', 'F12.20', 'F12.220', 'F12.221', 'F12.222', 'F12.229', 'F12.250', 'F12.251', 'F12.259', 'F12.280', 'F12.288', 'F12.29', 'F13.10', 'F13.120', 'F13.121', 'F13.129', 'F13.14', 'F13.150', 'F13.151', 'F13.159', 'F13.180', 'F13.181', 'F13.182', 'F13.188', 'F13.19', 'F13.20', 'F13.220', 'F13.221', 'F13.229', 'F13.230', 'F13.231', 'F13.232', 'F13.239', 'F13.24', 'F13.250', 'F13.251', 'F13.259', 'F13.26', 'F13.27', 'F13.280', 'F13.281', 'F13.282', 'F13.288', 'F13.29', 'F14.10', 'F14.120', 'F14.121', 'F14.122', 'F14.129', 'F14.14', 'F14.150', 'F14.151', 'F14.159', 'F14.180', 'F14.181', 'F14.182', 'F14.188', 'F14.19', 'F14.20', 'F14.220', 'F14.221', 'F14.222', 'F14.229', 'F14.23', 'F14.24', 'F14.250', 'F14.251', 'F14.259', 'F14.280', 'F14.281', 'F14.282', 'F14.288', 'F14.29', 'F15.10', 'F15.120', 'F15.121', 'F15.122', 'F15.129', 'F15.14', 'F15.150', 'F15.151', 'F15.159', 'F15.180', 'F15.181', 'F15.182', 'F15.188', 'F15.19', 'F15.20', 'F15.220', 'F15.221', 'F15.222', 'F15.229', 'F15.23', 'F15.24', 'F15.250', 'F15.251', 'F15.259', 'F15.280', 'F15.281', 'F15.282', 'F15.288', 'F15.29', 'F16.10', 'F16.120', 'F16.121', 'F16.122', 'F16.129', 'F16.14', 'F16.150', 'F16.151', 'F16.159', 'F16.180', 'F16.183', 'F16.188', 'F16.19', 'F16.20', 'F16.220', 'F16.221', 'F16.229', 'F16.24', 'F16.250', 'F16.251', 'F16.259', 'F16.280', 'F16.283', 'F16.288', 'F16.29', 'F18.10', 'F18.120', 'F18.121', 'F18.129', 'F18.14', 'F18.150', 'F18.151', 'F18.159', 'F18.17', 'F18.180', 'F18.188', 'F18.19', 'F18.20', 'F18.220', 'F18.221', 'F18.229', 'F18.24', 'F18.250', 'F18.251', 'F18.259', 'F18.27', 'F18.280', 'F18.288', 'F18.29', 'F19.10', 'F19.120', 'F19.121', 'F19.122', 'F19.129', 'F19.14', 'F19.150', 'F19.151', 'F19.159', 'F19.16', 'F19.17', 'F19.180', 'F19.181', 'F19.182', 'F19.188', 'F19.19', 'F19.20', 'F19.220', 'F19.221', 'F19.222', 'F19.229', 'F19.230', 'F19.231', 'F19.232', 'F19.239', 'F19.24', 'F19.250', 'F19.251', 'F19.259', 'F19.26', 'F19.27', 'F19.280', 'F19.281', 'F19.282', 'F19.288', 'F19.29')) aod_icd
    ON aod_icd.column_value = diag.icd_code
  JOIN DIM_DATE_TIME dt
    ON dt.datenum = diagnosis_dt_key AND dt.year = 2017
  JOIN fact_visits v
    ON v.network = diag.network AND v.visit_id = diag.visit_id AND final_visit_type_id IN (2, 3, 4)
  JOIN dim_patients p
    ON p.network = v.network AND p.patient_id = v.patient_id --AND TRUNC(sysdate, YEAR) > add_months(sysdate, -(13*12))
   AND FLOOR((add_months(trunc(sysdate,'year'),12)-1 - p.birthdate)/365) > 13
  WHERE diag.network = 'CBN'-- AND diag.patient_id = 119749 
  ORDER BY v.admission_dt
), 
tst_new AS
(
  SELECT 
    tst.network,	
    tst.visit_id,	
    tst.admission_dt,	
    lag(tst.admission_dt, 1) OVER (partition by tst.network, tst.patient_id order by tst.admission_dt) prev_visit_dt,
    round(tst.admission_dt - lag(tst.admission_dt, 1) OVER (partition by tst.network, tst.patient_id order by tst.admission_dt))  visit_days_diff,
    tst.icd_code,	
    tst.coding_scheme,	
    tst.patient_id,	
    tst.facility_key,	
    tst.problem_comments,	
    tst.diag_rnum,
    tst.birthdate
  FROM tst 
  WHERE diag_rnum = 1    
  ORDER BY admission_dt
)  
SELECT * FROM tst_new
WHERE visit_days_diff > 60;


select * from v_dsrip_pe006_results;
grant select on v_dsrip_pe006_results to public;
grant select on DSRIP_TR043_BH_VISITS_RPT_2017 to public;

select * from DSRIP_TR043_BH_VISITS_REPORT where mrn = 1072471
select * from DSRIP_TR043_BH_VISITS_REPORT where upper(patient_name) like '%GALE%'
grant select on DSRIP_TR043_BH_VISITS_REPORT to public;

ZP23605K	GALE, LAVAR	10/29/1979	M	1/6/2017	Tremont Quality Medical Care, P.C.	66977	ZP23605K	KC	2094028
ZP23605K	GALE, LAVAR	10/29/1979	M	1/6/2017	Tremont Quality Medical Care, P.C.	66977	ZP23605K	WO	1072471