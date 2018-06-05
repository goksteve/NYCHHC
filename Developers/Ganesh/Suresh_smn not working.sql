drop table tst_gk_pr2_smn;

--use_hash(pat v prc e f)
select count(1) from tst_gk_pr2_smn;
/* Formatted on 6/1/2018 12:17:02 (QP5 v5.287) */
CREATE TABLE tst_gk_pr2_smn
NOLOGGING
PARALLEL 32
AS
WITH 
dt AS
(
SELECT
  TO_DATE (EXTRACT (YEAR FROM SYSDATE) - 1 || '-04-01', 'YYYY-MM-DD') begin_dt,
  TRUNC (LAST_DAY (ADD_MONTHS (SYSDATE, -1))) end_dt,
  TRUNC (SYSDATE) report_period_start_dt
FROM dual

),
ptnt
AS 
(
  SELECT --+ materialize index(pr1 pk_problem) use_hash(pr pr1)
    COUNT (*) single, pr.patient_id, pr.network
  FROM problem_cmv pr, meta_conditions, problem pr1
  WHERE pr1.problem_number = pr.problem_number
    AND pr.network = pr1.network
    AND pr1.patient_id = pr.patient_id
    AND pr.code = VALUE
    AND coding_scheme_id IN (5, 10)
    AND criterion_id = 67
    AND status_id IN (0,6,7,8)
    AND onset_date <= TRUNC (LAST_DAY (ADD_MONTHS (SYSDATE, -1)))
  GROUP BY pr.patient_id, pr.network
),
        final_smn
        AS (SELECT --+ materialize
                   DISTINCT
                   dt.report_period_start_dt,
                   NVL (pat.medical_record_number, secondary_number) mrn,
                   NVL (visit_number, visit_secondary_number) visit_number,
                   pat.name patient_name,
                   TRUNC (pat.birthdate) AS dob,
                   pat.sex gender,
                   f.name facility,
                   TRUNC (admission_date_time) admission_date,
                   TRUNC (discharge_date_time) discharge_date,
                   vt.name visit_type,
                   pcp.prim_care_provider pcp,
                   NVL (prc.modified_proc_name, p.name) document_name,
                   TO_CHAR (e.date_time, 'Dy, dd Mon yy') document_date,
                   TO_CHAR (e.date_time, 'HH24MI') document_time,
                   CASE
                      WHEN     v.facility_id = 2
                           AND p.proc_id = 39118
                           AND es.name = 'complete'
                      THEN
                         'Y'
                      WHEN     v.facility_id = 1
                           AND modified_proc_name =
                                  '335 - Medicine Clinic  Ambcare Note (w/coding sheet)'
                           AND (SELECT MAX (ROWNUM)
                                  FROM result r, result_field rf
                                 WHERE     r.data_element_id =
                                              rf.data_element_id
                                       AND r.network = rf.network
                                       AND rf.name IN
                                              ('Treatment Goal (PCMH)',
                                               'Treatment Goal',
                                               'Treatment Goals',
                                               'Treatment Goals (Virology) PCMH',
                                               'Patient Lifestyle Goals',
                                               'Pt Lifestyle Goals',
                                               'Self Mgmt Plan',
                                               'Education/Counseling 0-11mos',
                                               'Education/Counseling 12-18yrs',
                                               'Education/Counseling 12-23mos',
                                               'Education/Counseling 2-3yrs',
                                               'Education/Counseling 4-6yrs',
                                               'Education/Counseling 7-11yrs',
                                               'Patient Education/Counseling',
                                               'Pt Ed/Counseling',
                                               'Pt Educ/Counseling',
                                               'Pt Education/Counseling',
                                               'Pt/Ed/Counseling',
                                               'PtEd/Counseling',
                                                  'Pt Ed'
                                               || CHR (38)
                                               || 'Counseling',
                                                  'Pt Education '
                                               || CHR (38)
                                               || ' Counseling',
                                                  'PtEd '
                                               || CHR (38)
                                               || ' Counseling',
                                               'Self Mgmt Goals (PCMH)')
                                       AND r.visit_id = e.visit_id
                                       AND r.event_id = e.event_id
                                       AND r.network = e.network) >= 3
                      THEN
                         'Y'
                   END
                      AS numerator,
                   CASE WHEN single = 1 THEN 'Y' ELSE 'N' END
                      AS singular_flag
              FROM dt
                   JOIN visit PARTITION (smn) v
                      ON     v.admission_date_time >= dt.begin_dt
                         AND v.admission_date_time <= dt.end_dt
                   JOIN visit_type vt
                      ON     v.visit_type_id = vt.visit_type_id
                         AND v.network = vt.network
                   JOIN ptnt
                      ON     ptnt.patient_id = v.patient_id
                         AND v.network = ptnt.network
                   JOIN proc_event PARTITION (smn) prc
                      ON     v.visit_id = prc.visit_id
                         AND prc.network = v.network
                   JOIN event PARTITION (smn) e
                      ON     prc.event_id = e.event_id
                         AND prc.visit_id = e.visit_id
                         AND prc.network = e.network
                   JOIN patient PARTITION (smn) pat
                      ON     pat.patient_id = ptnt.patient_id
                         AND pat.network = ptnt.network
                   JOIN hhc_patient_dimension pcp
                      ON     pcp.patient_id = pat.patient_id
                         AND pcp.network = pat.network
                   JOIN facility f
                      ON     v.facility_id = f.facility_id
                         AND v.network = f.network
                   JOIN event_status es
                      ON     e.event_status_id = es.event_status_id
                         AND e.network = es.network
                   JOIN proc PARTITION (smn) p
                      ON prc.proc_id = p.proc_id AND prc.network = p.network
                   LEFT JOIN visit_secondary_number PARTITION (smn) vsn
                      ON     v.visit_id = vsn.visit_id
                         AND v.network = vsn.network
                         AND vsn.visit_sec_nbr_type_id =
                                CASE
                                   WHEN v.facility_id = 1 THEN 15
                                   WHEN v.facility_id = 2 THEN 12
                                END
                   LEFT JOIN patient_secondary_number psn
                      ON     pat.patient_id = psn.patient_id
                         AND pat.network = psn.network
                         AND psn.secondary_nbr_type_id = 11
                   JOIN visit_segment_visit_location PARTITION (smn) vsvl
                      ON     vsvl.visit_id = v.visit_id
                         AND vsvl.visit_segment_number = 1
                         AND vsvl.network = v.network
                   JOIN hhc_location_dimension ld
                      ON     ld.location_id = vsvl.location_id
                         AND ld.network = vsvl.network
                   JOIN hhc_clinic_codes cc
                      ON cc.code = ld.clinic_code AND cc.network = ld.network
             WHERE     cc.code IN ('860',
                                   '861',
                                   '218',
                                   '223',
                                   '334',
                                   '335',
                                   '336',
                                   '337',
                                   '338',
                                   '339',
                                   '340',
                                   '341',
                                   '342',
                                   '346',
                                   '347',
                                   '348')
                   AND v.facility_id IN (1, 2))
SELECT * FROM final_smn;