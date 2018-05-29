DROP TABLE dsrip_tr045_ed_esi_4_5_rpt;

CREATE TABLE dsrip_tr045_ed_esi_4_5_rpt
AS
WITH dat
 -- 25-May-2018, GK: Count of visits per hour, day and month and per facility for Year - 2018
AS 
(
  SELECT --+ parallel(32)
    vst.visitnumber,
    f.facility,
    TO_CHAR (dt1.date_, 'Day') AS edvisitopenday,
    TO_CHAR (dt1.date_, 'Month-YYYY') AS edvisitopenmonth,
    EXTRACT(HOUR FROM CAST (dt1.date_ AS TIMESTAMP)) AS edvisitopendt_hr
    FROM edd_stg_patientvisitcorporate vst
    JOIN edd_stg_time dt1
      ON dt1.dimtimekey = vst.edvisitopendtkey AND vst.edvisitopendtkey != -1
    JOIN edd_stg_facilities f ON f.facilitykey = vst.facilitykey
    WHERE dt1  .date_ >= DATE '2018-01-01'
      AND dt1.date_ < DATE '2018-05-01'
      AND esikey IN (4, 5)
)
SELECT 
  *
FROM 
( 
  SELECT 
    visitnumber, facility, edvisitopenday AS dt, edvisitopenmonth AS mnth, edvisitopendt_hr
  FROM dat
)
PIVOT
(
  COUNT (visitnumber) FOR edvisitopendt_hr IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23)
)
ORDER BY 
  facility, TO_DATE (mnth, 'MONTH-YYYY'),
  CASE
    WHEN TRIM (dt) = 'SUNDAY' THEN 1
    WHEN TRIM (dt) = 'MONDAY' THEN 2
    WHEN TRIM (dt) = 'TUESDAY' THEN 3
    WHEN TRIM (dt) = 'WEDNESDAY' THEN 4
    WHEN TRIM (dt) = 'THURSDAY' THEN 5
    WHEN TRIM (dt) = 'FRIDAY' THEN 6
    WHEN TRIM (dt) = 'SATURDAY' THEN 7
  END ASC;