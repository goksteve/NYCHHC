CREATE OR REPLACE VIEW V_REF_PATIENT_SECONDARY_MRN AS
 SELECT /*+ PARALLEL (32) */
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
 WHERE  secondary_nbr_type_id IN (11,12,13,9,14,17)