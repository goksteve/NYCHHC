drop table STEVE_DEL_TR26 ;
CREATE TABLE STEVE_DEL_TR26 
NOLOGGING
AS

WITH report_dates AS
      (SELECT --+ materialize
        TRUNC(SYSDATE, 'MONTH') report_dt,
        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -24) start_dt,
        ADD_MONTHS(TRUNC(SYSDATE, 'MONTH'), -12) rslt_start_date,
        ADD_MONTHS(TRUNC((TRUNC(SYSDATE, 'MONTH') - 1), 'YEAR'), 12) - 1 AS report_year
       FROM
        DUAL),
     pat_diag  --31 have - DIAGNOSES:SCHIZOPHRENIA
     AS
     (
       SELECT --+  materialize 
       DISTINCT
       d.network, d.patient_id  --,  criterion_id AS crit_id, onset_date
       FROM    meta_conditions mc JOIN fact_patient_diagnoses d ON d.diag_code = mc.VALUE
       WHERE
       mc.criterion_id IN (31)AND d.status_id IN (0, 6, 7,8)
      ) ,

     visit_pat AS
      (
       SELECT distinct
        d.network,
        d.patient_id,
        d.order_dt,
        d.drug_description,
        d.dosage,
        d.frequency,
       NVL( a.drug_frequency_num_val,1) as daily_cnt,
        d.rx_quantity,
        d.rx_refills,
        COUNT(*) OVER ( PARTITION BY d.NETWORK, d.PATIENT_ID ) AS dispens_CNT
       FROM
        report_dates dt
        CROSS JOIN
          fact_patient_prescriptions d
         JOIN ref_drug_descriptions rd  ON rd.drug_description = d.drug_description AND rd.drug_type_id IN (106, 107, 108)
        JOIN pat_diag pd on pd.network = d.network and pd.patient_id  = d.patient_id
        LEFT JOIN ref_drug_frequency a  ON d.frequency LIKE a.drug_frequency
       WHERE  d.order_dt >= dt.start_dt AND d.order_dt < dt.report_dt
      )

SELECT  /*+ parallel (32 ) */
 *
FROM
 visit_pat



