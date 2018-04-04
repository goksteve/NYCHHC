DROP VIEW V_DSRIP_RPT_TR002_023_SUM_CDW;

CREATE OR REPLACE FORCE VIEW V_DSRIP_RPT_TR002_023_SUM_CDW
(
 "Reporting Period",
 "Facility name",
 "# Patients",
 "# Results",
 "% Results",
 "# A1c < 8",
 "% A1c < 8",
 "# A1c >= 8",
 "# A1c >= 9",
 "# A1c >= 9 or NULL"
) AS
 SELECT
     TO_CHAR(ADD_MONTHS(MAX(TRUNC(report_dt)), -12), 'MM/DD/YYYY')
  || ' - '
  || TO_CHAR(MAX(TRUNC(report_dt)) - 1, 'MM/DD/YYYY')
   "Reporting Period",
  DECODE(GROUPING(t.facility_name), 1, 'All Facilities', t.facility_name) "Facility name",
  COUNT(1) AS "# Patients",
  COUNT(a1c_final_calc_value) "# Results",
  ROUND(COUNT(a1c_final_calc_value) / COUNT(1), 4) AS "% Results",
  COUNT(a1c_less_8) "# A1c < 8",
  CASE
   WHEN COUNT(a1c_final_orig_value) > 0 THEN ROUND(COUNT(a1c_less_8) / COUNT(a1c_final_calc_value), 4)
   ELSE 0
  END
   AS "% A1c < 8",
  COUNT(a1c_more_8) "# A1c >= 8",
  COUNT(a1c_more_9) "# A1c >= 9",
  COUNT(a1c_more_9_null) "# A1c >= 9 or NULL"
 FROM
 DSRIP_TR002_023_A1C_CDW t
 WHERE
  TRUNC(t.report_dt) = (SELECT MAX(TRUNC(report_dt)) FROM DSRIP_TR002_023_A1C_CDW )
 GROUP BY
  ROLLUP(t.facility_name)
 ORDER BY
  GROUPING(t.facility_name), t.facility_name;
