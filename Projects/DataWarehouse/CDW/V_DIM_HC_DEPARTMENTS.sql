CREATE OR REPLACE VIEW v_dim_hc_departments AS
WITH
  -- 21-JUN-2018, SG, GK: Fixed speciality_code bug, where regexp_substr is failing to get the values.
  -- 31-JAN-2018, OK: created
  loc AS
  (
    SELECT
      LEVEL lvl,
      network, facility_id, location_id, name,
      SYS_CONNECT_BY_PATH(NVL(name, 'NA'), '~') path_name,
      CASE WHEN UPPER(bed) = 'TRUE' THEN 'Y' else 'N' END is_bed
    FROM location
    CONNECT BY network = PRIOR network AND parent_location_id = PRIOR location_id AND location_id <> PRIOR location_id 
    START WITH parent_location_id IS NULL OR location_id = '-1'
  ),
  area AS
  (
    SELECT
      loc.*,
      REGEXP_SUBSTR(path_name,'[^~]+') division,
      NVL(REGEXP_SUBSTR(path_name,'[^~]+', 1, 2), 'N/A') department,
      NVL(REGEXP_SUBSTR(path_name,'[^~]+', 1, 3), 'N/A') zone
    FROM loc
  ),
  dep AS
  (
    SELECT
      ar.network, ar.location_id, f.facility_key, 
      f.facility_name AS facility, ar.division, ar.department, ar.zone, ar.is_bed,
--      CASE
--        WHEN(division LIKE '%Interface%' OR division LIKE '%I/F%') AND LOWER(department) NOT LIKE 'shell%'
--          THEN TO_NUMBER(REGEXP_SUBSTR(department, '([0-9]+) *$', 1, 1, 'c', 1))
--      END specialty_code
      CASE
        WHEN(division LIKE '%Interface%' OR division LIKE '%I/F%') AND LOWER(department) NOT LIKE 'shell%'
        THEN 
          CASE
            --for departments that exists with a value, look for the department either begins with a number, or alternatively ends with a number
            WHEN REGEXP_LIKE(department, '(^\d{3}|\d{3}$)') AND department IS NOT NULL
    
            --if it does exists as above, extract the first 3 digit set if it begins with a number or extract last 3 digit set if doesn't begin with a letter
            THEN TO_NUMBER(REGEXP_SUBSTR(department, '(^\d{3}|\d{3}$)')) 
    
            --in case the 3 digit code exisits in the middle of the string, extract the first occurence of the 3 digit set. 
            ELSE TO_NUMBER(REGEXP_SUBSTR(department, '[[:digit:]]{3}'))
          END 
      END specialty_code
    FROM area ar
    JOIN dim_hc_facilities f ON f.network = ar.network AND f.facility_id = ar.facility_id
  )
SELECT
  dep.network,
  dep.location_id,
  dep.facility_key,
  dep.division,
  dep.department,
  dep.zone,
  dep.is_bed,
  dep.specialty_code,
  NVL(c.description, 'N/A') AS specialty,
  NVL(c.service, 'N/A') service,
  NVL(s.service_type, 'N/A') service_type,
  'QCPR' AS source
FROM dep
LEFT JOIN hhc_clinic_codes c
  ON c.network = dep.network AND c.code = dep.specialty_code
LEFT JOIN ref_hc_specialties s
  ON s.code = dep.specialty_code;
