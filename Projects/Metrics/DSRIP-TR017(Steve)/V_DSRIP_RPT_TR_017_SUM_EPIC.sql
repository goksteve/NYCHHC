CREATE OR REPLACE VIEW v_dsrip_rpt_tr_017_sum_epic AS
 SELECT
  DECODE(GROUPING(t.location_name), 1, 'All Locations', t.location_name) "Location name",
  COUNT(DISTINCT mrn_empi) AS "# Patients",
  COUNT(numr_flag_ldl_c_and_hemo_test) / 2 AS "# Patient with Both Results",
  CASE
   WHEN COUNT(DISTINCT mrn_empi) > 0 THEN
    ROUND((COUNT(numr_flag_ldl_c_and_hemo_test) / 2) / COUNT(DISTINCT mrn_empi), 4)
   ELSE
    0
  END
   AS "% Patient with Both Results",
  COUNT(hemoglobin_result_value) AS "# Patient with A1C Only ",
  COUNT(ldl_c_result_value) AS "# Patient with LDL Only "
 FROM
  dsrip_tr_017_diab_mon_epic t
 WHERE
  report_dt = (SELECT MAX(report_dt) FROM dsrip_tr_017_diab_mon_epic)
 GROUP BY
  ROLLUP(t.location_name)
 ORDER BY
  GROUPING(t.location_name), t.location_name;