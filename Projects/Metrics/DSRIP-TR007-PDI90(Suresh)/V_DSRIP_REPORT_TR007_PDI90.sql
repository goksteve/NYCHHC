CREATE OR REPLACE VIEW V_DSRIP_REPORT_TR007_PDI90
AS
WITH 
-- 23-Aug-2018, SR: created
dt AS
(
  SELECT --+ materialize
    TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH') AS REPORT_DATE,
    ADD_MONTHS(TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH'), -1) REPORT_BEGIN_DATE,
    TRUNC(NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH') - 1 REPORT_END_DATE 
  FROM
  dual
),
pd AS 
(
  SELECT 
    REPORT_DATE,
    REPORT_BEGIN_DATE,
    REPORT_END_DATE,
    CASE
       WHEN     EXISTS
                   (SELECT 'X'
                      FROM fact_visit_diagnoses cmv,
                           meta_conditions meta
                     WHERE     cmv.network = v.network
                           AND cmv.visit_id = v.visit_id
                           AND REPLACE (cmv.icd_code, '.', '') =
                                  meta.VALUE
                           AND include_exclude_ind = 'I'
                           AND meta.criterion_id = 59
                           AND cmv.coding_scheme = 'ICD-10')
            AND NOT EXISTS
                   (SELECT 'X'
                      FROM fact_visit_diagnoses cmv,
                           meta_conditions meta
                     WHERE     cmv.network = v.network
                           AND cmv.visit_id = v.visit_id
                           AND REPLACE (cmv.icd_code, '.', '') =
                                  meta.VALUE
                           AND meta.criterion_id = 59
                           AND include_exclude_ind = 'E'
                           AND cmv.coding_scheme = 'ICD-10')
       THEN
          1
    END       AS pdi_14_flag,
    CASE
       WHEN EXISTS
               (SELECT 'X'
                  FROM fact_visit_diagnoses cmv,
                       meta_conditions meta
                 WHERE     cmv.network = v.network
                       AND cmv.visit_id = v.visit_id
                       AND REPLACE (cmv.icd_code, '.', '') =
                              meta.VALUE
                       AND include_exclude_ind = 'I'
                       AND meta.criterion_id = 60
                       AND cmv.coding_scheme = 'ICD-10')
       THEN
          1
    END AS pdi_15_flag,
    CASE
       WHEN     EXISTS
                   (SELECT 'X'
                      FROM fact_visit_diagnoses cmv,
                           meta_conditions meta
                     WHERE     cmv.network = v.network
                           AND cmv.visit_id = v.visit_id
                           AND REPLACE (cmv.icd_code, '.', '') =
                                  meta.VALUE
                           AND include_exclude_ind = 'I'
                           AND meta.criterion_id = 61
                           AND cmv.coding_scheme = 'ICD-10')
            AND NOT EXISTS
                   (SELECT 'X'
                      FROM fact_visit_diagnoses cmv,
                           meta_conditions meta
                     WHERE     cmv.network = v.network
                           AND cmv.visit_id = v.visit_id
                           AND REPLACE (cmv.icd_code, '.', '') =
                                  meta.VALUE
                           AND include_exclude_ind = 'E'
                           AND meta.criterion_id = 61
                           AND cmv.coding_scheme = 'ICD-10')
       THEN
          1
    END AS pdi_16_flag,
    CASE
       WHEN     EXISTS
                   (SELECT 'X'
                      FROM fact_visit_diagnoses cmv,
                           meta_conditions meta
                     WHERE     cmv.network = v.network
                           AND cmv.visit_id = v.visit_id
                           AND REPLACE (cmv.icd_code, '.', '') =
                                  meta.VALUE
                           AND include_exclude_ind = 'I'
                           AND meta.criterion_id = 62
                           AND cmv.coding_scheme = 'ICD-10')
            AND NOT EXISTS
                   (SELECT 'X'
                      FROM fact_visit_diagnoses cmv,
                           meta_conditions meta
                     WHERE     cmv.network = v.network
                           AND cmv.visit_id = v.visit_id
                           AND REPLACE (cmv.icd_code, '.', '') =
                                  meta.VALUE
                           AND include_exclude_ind = 'E'
                           AND meta.criterion_id = 62
                           AND cmv.coding_scheme = 'ICD-10')
       THEN
          1
    END AS pdi_18_flag,
    CASE
       WHEN     FLOOR (
                   MONTHS_BETWEEN (
                        ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'), -1)
                      + 30,
                      birthdate)) >= 3
            AND FLOOR (
                     MONTHS_BETWEEN (
                          ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'),
                                      -1)
                        + 30,
                        TRUNC (p.birthdate))
                   / 12) <= 5
       THEN
          '3 M to 5 Years'
       WHEN     FLOOR (
                     MONTHS_BETWEEN (
                          ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'),
                                      -1)
                        + 30,
                        TRUNC (p.birthdate))
                   / 12) >= 6
            AND FLOOR (
                     MONTHS_BETWEEN (
                          ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'),
                                      -1)
                        + 30,
                        TRUNC (p.birthdate))
                   / 12) <= 17
       THEN
          '6 to 17 Years'
    END
       age_group,
    v.network,
    fd.facility_name,
    p.name patient_name,
    p.apt_suite apt_suite,
    p.street_address street_address,
    p.city city,
    p.state state,
    p.country country,
    p.mailing_code zip_code,
    p.home_phone home_phone,
    p.day_phone cell_phone,
    v.visit_id,
    NVL (REGEXP_SUBSTR (v.visit_number, '^[^-]*'), mdm.mrn) mrn,
    TRUNC (p.birthdate) patient_dob,
    v.visit_number,
    p.patient_id,
    v.admission_dt admission_date,
    v.discharge_dt discharge_time,
    fc.financial_class_name financial_class,
    p.payer_name,
    p.payer_group,
    p.pcp_provider_name prim_care_provider,
    prv1.provider_name attending_provider,
    prv2.provider_name resident_provider,
    CASE
       WHEN     FLOOR (
                   MONTHS_BETWEEN (
                        ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'), -1)
                      + 30,
                      p.birthdate)) >= 3
            AND FLOOR (
                   MONTHS_BETWEEN (
                        ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'), -1)
                      + 30,
                      p.birthdate)) <= 12
       THEN
             FLOOR (
                MONTHS_BETWEEN (
                   ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'), -1) + 30,
                   p.birthdate))
          || ' Months'
       ELSE
             FLOOR (
                  MONTHS_BETWEEN (
                       ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'), -1)
                     + 30,
                     TRUNC (p.birthdate))
                / 12)
          || ' Years'
    END
       AS age
  FROM 
  dt
  join fact_visits v
    on v.admission_dt >= dt.REPORT_BEGIN_DATE and v.admission_dt < dt.REPORT_DATE
  JOIN dim_patients p
    ON p.network = v.network AND p.patient_id = v.patient_id
  LEFT JOIN dim_hc_facilities fd
    ON fd.facility_key = v.facility_key
  LEFT JOIN dconv.mdm_qcpr_pt_02122016 mdm
    ON     mdm.network = v.network
  AND TO_NUMBER (mdm.patientid) = v.patient_id
  AND SUBSTR (mdm.facility_name, 1, 2) = fd.facility_cd
  AND mdm.epic_flag = 'N'
  AND mdm.dc_flag IS NULL
  LEFT JOIN cdw.ref_financial_class fc
    ON     fc.network = v.network
  AND fc.financial_class_id = v.financial_class_id
  LEFT JOIN dim_providers prv1
   ON prv1.provider_key = v.attending_provider_key
  LEFT JOIN dim_providers prv2
   ON prv1.provider_key = v.resident_provider_key
  LEFT JOIN dim_payers p 
    ON p.payer_key = v.first_payer_key
  WHERE     v.final_visit_type_id = 1
    AND v.network NOT IN ('QHN', 'SBN')
    AND FLOOR(MONTHS_BETWEEN (ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'), -1) + 30, birthdate)) >= 3
    AND FLOOR(MONTHS_BETWEEN (ADD_MONTHS (TRUNC (dt.REPORT_END_DATE, 'YEAR'), -1) + 30,TRUNC (p.birthdate))/ 12) <= 17
    AND p.current_flag = 1
),
pcp_info AS 
(
  SELECT --+ materialize
    v.network,
    v.patient_id,
    v.visit_id pcp_visit_id,
    v.admission_dt pcp_visit_dt,
    fd.facility_name AS pcp_vst_facility_name,
    ROW_NUMBER ()
    OVER (PARTITION BY v.network, v.patient_id
    ORDER BY v.admission_dt DESC)
    rnum
  FROM pd vst
  JOIN cdw.fact_visits v
    ON     v.network = vst.network
   AND v.patient_id = vst.patient_id
  JOIN cdw.dim_hc_departments d
    ON     d.department_key = v.last_department_key
   AND d.service_type = 'PCP'
  JOIN dim_hc_facilities fd 
  ON fd.facility_key = v.facility_key
),
pd1 AS 
(
  SELECT                                                
    DISTINCT
    v.*,
    CASE
     WHEN pdi_14_flag = 1
     THEN
        (  SELECT LISTAGG (icd_code || problem_comments, ' || ')
                     WITHIN GROUP (ORDER BY problem_nbr)
             FROM fact_visit_diagnoses cmv, meta_conditions meta
            WHERE     cmv.network = v.network
                  AND cmv.visit_id = v.visit_id
                  AND REPLACE (cmv.icd_code, '.', '') =
                         meta.VALUE
                  AND include_exclude_ind = 'I'
                  AND meta.criterion_id = 59
                  AND cmv.coding_scheme = 'ICD-10'
         GROUP BY v.visit_id)
    END AS pdi_14_inclusions,
    CASE
     WHEN pdi_15_flag = 1
     THEN
        (  SELECT LISTAGG (icd_code || problem_comments, ' || ')
                     WITHIN GROUP (ORDER BY problem_nbr)
             FROM fact_visit_diagnoses cmv, meta_conditions meta
            WHERE     cmv.network = v.network
                  AND cmv.visit_id = v.visit_id
                  AND REPLACE (cmv.icd_code, '.', '') =
                         meta.VALUE
                  AND include_exclude_ind = 'I'
                  AND meta.criterion_id = 60
                  AND cmv.coding_scheme = 'ICD-10'
         GROUP BY v.visit_id)
    END AS pdi_15_inclusions,
    CASE
     WHEN pdi_16_flag = 1
     THEN
        (  SELECT LISTAGG (icd_code || problem_comments, ' || ')
                     WITHIN GROUP (ORDER BY problem_nbr)
             FROM fact_visit_diagnoses cmv, meta_conditions meta
            WHERE     cmv.network = v.network
                  AND cmv.visit_id = v.visit_id
                  AND REPLACE (cmv.icd_code, '.', '') =
                         meta.VALUE
                  AND include_exclude_ind = 'I'
                  AND meta.criterion_id = 62
                  AND cmv.coding_scheme = 'ICD-10'
         GROUP BY v.visit_id)
    END AS pdi_16_inclusions,
    CASE
     WHEN pdi_18_flag = 1
     THEN
        (  SELECT LISTAGG (icd_code || problem_comments, ' || ')
                     WITHIN GROUP (ORDER BY problem_nbr)
             FROM fact_visit_diagnoses cmv, meta_conditions meta
            WHERE     cmv.network = v.network
                  AND cmv.visit_id = v.visit_id
                  AND REPLACE (cmv.icd_code, '.', '') =
                         meta.VALUE
                  AND include_exclude_ind = 'I'
                  AND meta.criterion_id = 62
                  AND cmv.coding_scheme = 'ICD-10'
         GROUP BY v.visit_id)
    END AS pdi_18_inclusions,
    pcp.pcp_visit_id,
    pcp.pcp_visit_dt,
    pcp.pcp_vst_facility_name,
    CASE
     WHEN    v.payer_group IN ('Medicaid', 'Medicare')
          OR v.payer_group IS NULL
     THEN
        payer_group
     WHEN v.payer_group = 'UNINSURED'
     THEN
        'Self pay'
     ELSE
        'Commercial'
    END payer_type
  FROM pd v
  LEFT JOIN pcp_info pcp
    ON     pcp.network = v.network
   AND pcp.patient_id = v.patient_id
   AND pcp.rnum = 1
)
SELECT * FROM pd1;