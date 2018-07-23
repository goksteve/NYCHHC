SELECT 
  *
FROM 
(  
  SELECT --+ parallel(32)
    facility, TO_CHAR (arrival_dt, 'DAY') day, TRUNC (arrival_dt) dt, TO_CHAR (arrival_dt, 'HH24') hr, NVL (COUNT (*), 0) vst_cnt
  FROM edd_fact_visits v
  JOIN edd_dim_facilities f ON v.facility_key = f.facilitykey
  WHERE TRUNC (arrival_dt) >= '01-JUN-2018' AND TRUNC (arrival_dt) < '01-JUL-2018' AND esi_key IN (4, 5) --AND facility = 'Bellevue'
  GROUP BY (facility, TO_CHAR (arrival_dt, 'DAY'), TRUNC (arrival_dt), TO_CHAR (arrival_dt, 'HH24'))
)
PIVOT
(
  MAX (NVL (vst_cnt, 0)) FOR hr IN 
  (
    '00' AS "00:00", '01' AS "01:00", '02' AS "02:00", '03' AS "03:00", '04' AS "04:00", '05' AS "05:00", '06' AS "06:00", '07' AS "07:00", '08' AS "08:00", '09' AS "09:00",
    '10' AS "10:00", '11' AS "11:00", '12' AS "12:00", '13' AS "13:00", '14' AS "14:00", '15' AS "15:00", '16' AS "16:00", '17' AS "17:00", '18' AS "18:00", '19' AS "19:00",
    '20' AS "20:00", '21' AS "21:00", '22' AS "22:00", '23' AS "23:00"
  )
)
ORDER BY facility;