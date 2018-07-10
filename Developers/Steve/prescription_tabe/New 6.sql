SELECT
NETWORK
 pm.patient_id,
 pm.medication_id,
 pma.medication_archive_id,
 pma.assoc_visit_id,
 pma.processing_mode,
 pm.order_emp_provider_id,
 pm.order_emp_provider_string,
 pm.stop_date_approx,
 pm.frequency,
 pm.quantity,
 pm.refills,
 pm.allow_substitution,
 pm.comments,
 pm.continue_comment,
 pm.rx_disposition,
 pm.order_date_time,
 pm.start_date,
 pm.stop_date,
 pma.archive_date_time,
 pm.dkv_ahfs_class_name,
 pm.dkv_drug_name,
 pm.dkv_dnid_name,
 pm.dkv_generic_dnid_name,
 pm.dkv_product_string,
 pm.dosage,
 pmam.dosage AS pmam_dosage,
 pm.last_action_emp_provider_id,
 pm.e_rx_status_id,
 pm.e_rx_disposition,
 pm.active_flag,
 pm.reason_string,
 pm.reason_not_continued_free_text,
 pm.reason_not_continued_cds_id,
 pmam.last_action_taken_id,
 lat.name AS last_action_taken,
 MAX(pm.order_date_time) OVER (PARTITION BY pm.patient_id) AS latest_order_date,
 MAX(pma.archive_date_time) OVER (PARTITION BY pm.patient_id) AS latest_archive_date,
 MAX(pmam.medication_id) OVER (PARTITION BY pm.patient_id) AS last_med_id,
 MAX(pma.medication_archive_id) OVER (PARTITION BY pm.patient_id, pmam.medication_id)
  AS last_medication_archive_id
FROM
 patient_med pm,
 patient_med_archive_med pmam,
 patient_med_archive pma,
 patient_med_last_action_taken lat
WHERE
 1 = 1
 AND pm.patient_id = pmam.patient_id
 AND pm.medication_id = pmam.medication_id
 AND pmam.patient_id = pma.patient_id
 AND pmam.medication_archive_id = pma.medication_archive_id
 AND pmam.last_action_taken_id = lat.last_action_taken_id(+) -- outer join because some last_action_taken_ids are '0'
 AND ((pma.archive_date_time >= '01-NOV-2017' 
       OR pmam.order_date_time >='01-NOV-2017' )
      AND pmam.last_action_taken_id <> 4) -- last_action_taken_id = 4 is 'discontinued'
 --pm.order_date_time between '01-jul-2017' and '01-jan-2018'
 AND pm.patient_id = 192;