CREATE OR REPLACE VIEW v_ref_patient_secondary_mrn AS
 WITH sec_nbr AS
       (SELECT /*+ materialize) */
         network,
         patient_id,
         secondary_number AS second_mrn,
         CASE
          WHEN (network = 'GP1' AND secondary_nbr_type_id = 13) THEN 1
          WHEN (network = 'GP1' AND secondary_nbr_type_id = 11) THEN 2
          WHEN (network = 'GP1' AND secondary_nbr_type_id = 12) THEN 3
          WHEN (network = 'CBN' AND secondary_nbr_type_id = 12) THEN 4
          WHEN (network = 'CBN' AND secondary_nbr_type_id = 13) THEN 5
          WHEN (network = 'NBN' AND secondary_nbr_type_id = 9) THEN 2
          WHEN (network = 'NBX' AND secondary_nbr_type_id = 11) THEN 2
          WHEN (network = 'QHN' AND secondary_nbr_type_id = 11) THEN 2
          WHEN (network = 'SBN' AND secondary_nbr_type_id = 11) THEN 1
          WHEN (network = 'SMN' AND secondary_nbr_type_id = 11) THEN 2
          WHEN (network = 'SMN' AND secondary_nbr_type_id = 13) THEN 7
          WHEN (network = 'SMN' AND secondary_nbr_type_id = 14) THEN 8
          WHEN (network = 'SMN' AND secondary_nbr_type_id = 17) THEN 9
          ELSE NULL
         END
          AS facility_id
        FROM
         patient_secondary_number
        WHERE
         secondary_nbr_type_id IN (11,
                                   12,
                                   13,
                                   9,
                                   14,
                                   17))
 SELECT /*+ parallel (32) */
  distinct n.network,
  n.patient_id,
  TRIM(n.second_mrn) second_mrn,
  f.facility_key,
  n.facility_id
 FROM
  dim_hc_facilities f JOIN sec_nbr n ON n.network = f.network AND n.facility_id = f.facility_id
 WHERE
  n.facility_id IS NOT NULL AND (n.second_mrn IS NOT NULL AND TRIM(n.second_mrn) <> '*')