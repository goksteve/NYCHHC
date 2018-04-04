CREATE OR REPLACE FORCE VIEW v_tr002_tr023_sum_cdw AS
    SELECT TO_CHAR(ADD_MONTHS(MAX(TRUNC(report_dt)), -12), 'MM/DD/YYYY') || ' - ' || TO_CHAR(MAX(TRUNC(report_dt)) - 1, 'MM/DD/YYYY')
              "Reporting Period",
           DECODE(GROUPING(t.facility_name), 1, 'All Facilities', t.facility_name) "Facility name",
           COUNT(1) AS "# Patients",
           COUNT(A1C_FINAL_CALC_VALUE) "# Results",
           ROUND(COUNT(A1C_FINAL_CALC_VALUE) / COUNT(1), 4) AS "% Results",
           COUNT(a1c_less_8) "# A1c < 8",
           CASE WHEN COUNT(A1C_FINAL_ORIG_VALUE) > 0 THEN ROUND(COUNT(a1c_less_8) / COUNT(A1C_FINAL_CALC_VALUE), 4) ELSE 0 END AS "% A1c < 8",
           COUNT(a1c_more_8) "# A1c >= 8",
           COUNT(a1c_more_9) "# A1c >= 9",
           COUNT(a1c_more_9_null) "# A1c >= 9 or NULL"
    FROM   DSRIP_TR_002_023_HBA1C t
    WHERE TRUNC(t.report_dt) = (SELECT MAX(TRUNC(report_dt)) FROM DSRIP_TR_002_023_HBA1C)
    GROUP BY ROLLUP(t.facility_name)
    ORDER BY GROUPING(t.facility_name), t.facility_name;