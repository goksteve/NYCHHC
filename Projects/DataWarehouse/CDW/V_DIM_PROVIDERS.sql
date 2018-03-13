CREATE OR REPLACE VIEW v_dim_providers AS
WITH
 -- 13-Mar-2018, GK, SG, OK: team work
  prov_data AS 
  (
    SELECT network,
      emp_provider_id,
      provider_name,
      title_id,
      title_name,
      title_prefix,
      title_suffix,
      physician_flag,
      physician_group_id,
      physician_group_name,
      emp_id,
      license_id,
      social_security_id,
      sdg_emp_no_id,
      prac_npi_id,
      npi_id,
      license_exp_date_id
    FROM 
    (
      SELECT 
        network,
        emp_provider_id,
        provider_name,
        title_id,
        title_name,
        title_prefix,
        title_suffix,
        physician_flag,
        physician_group_id,
        physician_group_name,
        external_number_id,
        VALUE
      FROM 
      (
        SELECT 
          ep.network,
          ep.emp_provider_id,
          ep.name AS provider_name,
          ep.title_id,
          t.name AS title_name,
          t.prefix AS title_prefix,
          t.suffix AS title_suffix,
          ep.physician AS physician_flag,
          ep.physician_group_id,
          pg.name AS physician_group_name,
          ee.external_number_id,
          ee.VALUE,
          ROW_NUMBER()OVER (PARTITION BY ep.network,ep.emp_provider_id,ee.external_number_id ORDER BY ee.facility_id ASC) rnb
        FROM emp_provider ep
        LEFT JOIN emp_facility_external_number ee
          ON ep.emp_provider_id = ee.emp_provider_id
         AND ep.network = ee.network
         AND ee.external_number_id IN('2','4','27','29','36','39','40')
        LEFT JOIN physician_group pg ON ep.physician_group_id =pg.physician_group_id AND ep.network = pg.network
        LEFT JOIN title t ON ep.title_id = t.title_id AND ep.network = t.network
      )
      WHERE rnb = 1
      AND provider_name IS NOT NULL
      AND emp_provider_id > 0
    )
    PIVOT
    (
      MAX(VALUE) id
      FOR external_number_id IN 
      (
        '2' AS emp,
        '4' AS license,
        '27' AS social_security,
        '29' AS sdg_emp_no,
        '36' AS prac_npi,
        '39' AS npi,
        '40' AS license_exp_date
      )
    )
  )
SELECT 
  p.network,
  p.provider_id,
  d.provider_key,
  d.archive_number,
  d.ROWID row_id,
  p.provider_name,
  p.title_id,
  p.title_name,
  p.title_prefix,
  p.title_suffix,
  p.physician_flag,
  p.emp,
  p.license,
  p.social_security,
  p.sdg_emp_no,
  p.prac_npi,
  p.npi,
  p.license_exp_date_id,
  p.physician_service_id,
  p.physician_service_name,
  p.physician_service_id_1,
  p.physician_service_name_1,
  p.physician_service_id_2,
  p.physician_service_name_2,
  p.physician_service_id_3,
  p.physician_service_name_3,
  p.physician_service_id_4,
  p.physician_service_name_4,
  p.physician_service_id_5,
  p.physician_service_name_5,
  'QCPR' AS source,
  CASE
    WHEN d.provider_id IS NULL THEN 'NEW'
    WHEN NVL (d.provider_name, '$$N/A$$') <> p.provider_name THEN 'NAME'
    WHEN NVL (d.title_id, -101010101) <> p.title_id  THEN  'TITLE_ID'
    WHEN NVL (d.title_name, '$$N/A$$') <> p.title_name    THEN  'TITLE_NAME'
    WHEN NVL (d.title_prefix, '$$N/A$$') <> p.title_prefix  THEN  'TITLE_PREFIX'
    WHEN NVL (d.title_suffix, '$$N/A$$') <> p.title_suffix  THEN  'TITLE_PREFIX'
    WHEN NVL (d.physician_flag, '$$N/A$$') <> p.physician_flag  THEN  'PHYSICIAN_FLAG'
    WHEN NVL (d.emp, '$$N/A$$') <> p.emp  THEN  'EMP_ID'
    WHEN NVL (d.license, '$$N/A$$') <> p.license  THEN  'LICENSE_VALUE'
    WHEN NVL (d.social_security, '$$N/A$$') <> p.social_security   THEN   'SOCIAL_SECURITY_VALUE'
    WHEN NVL (d.sdg_emp_no, '$$N/A$$') <> p.sdg_emp_no  THEN  'SDG_EMP_NO_VALUE'
    WHEN NVL (d.prac_npi, '$$N/A$$') <> p.prac_npi  THEN  'PRAC_NPI_VALUE'
    WHEN NVL (d.npi, '$$N/A$$') <> p.npi  THEN  'NPI_VALUE'
    WHEN NVL (d.license_exp_date_id, '$$N/A$$') <>  p.license_exp_date_id  THEN  'LICENSE_EXP_DATE_VALUE'
    WHEN NVL (d.physician_service_id, -101010101) <>  p.physician_service_id  THEN  'PHYSICIAN_SERVICE_ID'
    WHEN NVL (d.physician_service_name, '$$N/A$$') <>  p.physician_service_name  THEN  'PHYSICIAN_SERVICE_NAME'
    WHEN NVL (d.physician_service_id_1, -101010101) <>  p.physician_service_id_1  THEN  'PHYSICIAN_SERVICE_ID_1'
    WHEN NVL (d.physician_service_name_1, '$$N/A$$') <>  p.physician_service_name_1  THEN  'PHYSICIAN_SERVICE_NAME_1'
    WHEN NVL (d.physician_service_id_2, -101010101) <>  p.physician_service_id_2  THEN  'PHYSICIAN_SERVICE_ID_2'
    WHEN NVL (d.physician_service_name_2, '$$N/A$$') <>  p.physician_service_name_2  THEN  'PHYSICIAN_SERVICE_NAME_2'
    WHEN NVL (d.physician_service_id_3, -101010101) <>  p.physician_service_id_3  THEN  'PHYSICIAN_SERVICE_ID_3'
    WHEN NVL (d.physician_service_name_3, '$$N/A$$') <>  p.physician_service_name_3  THEN  'PHYSICIAN_SERVICE_NAME_3'
    WHEN NVL (d.physician_service_id_4, -101010101) <>  p.physician_service_id_4  THEN  'PHYSICIAN_SERVICE_ID_4'
    WHEN NVL (d.physician_service_name_4, '$$N/A$$') <>  p.physician_service_name_4  THEN  'PHYSICIAN_SERVICE_NAME_4'
    WHEN NVL (d.physician_service_id_5, -101010101) <>  p.physician_service_id_5  THEN  'PHYSICIAN_SERVICE_ID_5'
    WHEN NVL (d.physician_service_name_5, '$$N/A$$') <>  p.physician_service_name_5  THEN  'PHYSICIAN_SERVICE_NAME_5'
  END change
FROM 
(
  SELECT 
    DISTINCT fi.network,
    fi.provider_id,
    fi.provider_name,
    fi.title_id,
    fi.title_name,
    fi.title_prefix,
    fi.title_suffix,
    fi.physician_flag,
    fi.emp,
    fi.license,
    fi.social_security,
    fi.sdg_emp_no,
    fi.prac_npi,
    fi.npi,
    fi.license_exp_date_id,
    fi.physician_service_id,
    fi.physician_service_name,
    CASE 
      WHEN INSTR (fi.physician_service_id,'|',1,1) > 0
      THEN SUBSTR (fi.physician_service_id,1,INSTR (fi.physician_service_id,'|',1,1)- 1)
      ELSE fi.physician_service_id
    END AS physician_service_id_1,
    CASE
      WHEN INSTR (fi.physician_service_name,'|',1,1) > 0
      THEN SUBSTR (fi.physician_service_name,1,INSTR (fi.physician_service_name,'|',1,1)- 1)
      ELSE fi.physician_service_name
    END AS physician_service_name_1,
    CASE
      WHEN INSTR (fi.physician_service_id,'|',1,1) > 0
      THEN 
        CASE 
          WHEN INSTR (fi.physician_service_id,'|',1,2) > 0
          THEN SUBSTR (fi.physician_service_id,INSTR (fi.physician_service_id, '|') + 1,INSTR (fi.physician_service_id,'|',1,2)- INSTR (fi.physician_service_id, '|')- 1)
          ELSE SUBSTR (fi.physician_service_id,INSTR (fi.physician_service_id,'|',1,1)+ 1,LENGTH (fi.physician_service_id)- INSTR (fi.physician_service_id,'|',1,1))
        END
      ELSE
        NULL
    END AS physician_service_id_2,
    CASE
      WHEN INSTR (fi.physician_service_id,'|',1,1) > 0
      THEN
        CASE
          WHEN INSTR(fi.physician_service_name,'|',1, 2) > 0
          THEN SUBSTR(fi.physician_service_name,INSTR (fi.physician_service_name, '|') + 1,INSTR (fi.physician_service_name,'|',1,2)- INSTR (fi.physician_service_name, '|')- 1)
          ELSE SUBSTR (fi.physician_service_name,INSTR (fi.physician_service_name,'|',1,1)+ 1,LENGTH (fi.physician_service_name)- INSTR (fi.physician_service_name,'|',1,1))
        END
      ELSE NULL
    END AS physician_service_name_2,
    CASE
      WHEN INSTR (fi.physician_service_name,'|',1,2) > 0
      THEN
        CASE
          WHEN INSTR (fi.physician_service_id,'|',1,3) > 0  
          THEN SUBSTR (fi.physician_service_id,INSTR (fi.physician_service_id,'|',1,2)+ 1,INSTR (fi.physician_service_id,'|',1,3)-INSTR (fi.physician_service_id,'|',1,2)- 1)
          ELSE SUBSTR (fi.physician_service_id,INSTR (fi.physician_service_id,'|',1,2)+ 1,LENGTH (fi.physician_service_id)- INSTR (fi.physician_service_id,'|',1,2))
        END
      ELSE NULL
    END AS physician_service_id_3,
    CASE
      WHEN INSTR (fi.physician_service_name,'|',1,2) > 0
      THEN  
        CASE
          WHEN INSTR (fi.physician_service_name,'|',1,3) > 0
          THEN SUBSTR (fi.physician_service_name,INSTR (fi.physician_service_name,'|',1,2)+ 1, INSTR (fi.physician_service_name,'|',1,3)- INSTR (fi.physician_service_name,'|',1,2)- 1)
          ELSE SUBSTR (fi.physician_service_name,INSTR (fi.physician_service_name,'|',1,2)+ 1,LENGTH (fi.physician_service_name)- INSTR (fi.physician_service_name,'|',1,2))
        END
      ELSE
        NULL
    END AS physician_service_name_3,
    CASE
      WHEN INSTR (fi.physician_service_name,'|',1,3) > 0
      THEN  
        CASE
          WHEN INSTR (fi.physician_service_id,'|',1,4) > 0
          THEN SUBSTR (fi.physician_service_id,INSTR (fi.physician_service_id,'|',1,3)+ 1,INSTR (fi.physician_service_id,'|',1,4)- INSTR (fi.physician_service_id,'|',1,3)- 1)
          ELSE SUBSTR (fi.physician_service_id,INSTR (fi.physician_service_id,'|',1,3)+ 1,LENGTH (fi.physician_service_id)- INSTR (fi.physician_service_id,'|',1,3))
        END
      ELSE NULL
    END AS physician_service_id_4,
    CASE
      WHEN INSTR (fi.physician_service_name,'|',1, 3) > 0
      THEN  
        CASE
          WHEN INSTR (fi.physician_service_name,'|',1,4) > 0
          THEN SUBSTR (fi.physician_service_name,INSTR (fi.physician_service_name,'|',1,3)+ 1,INSTR (fi.physician_service_name,'|',1,4)- INSTR (fi.physician_service_name,'|',1,3)- 1)
          ELSE SUBSTR (fi.physician_service_name,INSTR (fi.physician_service_name,'|',1,3)+ 1,LENGTH (fi.physician_service_name)- INSTR (fi.physician_service_name,'|',1,3))
        END
      ELSE NULL
    END AS physician_service_name_4,
    CASE
      WHEN INSTR (fi.physician_service_name,'|',1,4) > 0
      THEN  
        CASE
          WHEN INSTR (fi.physician_service_id,'|',1,5) > 0
          THEN SUBSTR (fi.physician_service_id,INSTR (fi.physician_service_id,'|',1,4)+ 1,INSTR (fi.physician_service_id,'|',1,5)- INSTR (fi.physician_service_id,'|',1,4)- 1)
          ELSE SUBSTR (fi.physician_service_id,INSTR (fi.physician_service_id,'|',1,4)+ 1,LENGTH (fi.physician_service_id)- INSTR (fi.physician_service_id,'|',1,4))
        END
      ELSE NULL
    END AS physician_service_id_5,
    CASE
      WHEN INSTR (fi.physician_service_name,'|',1,4) > 0
      THEN
        CASE
          WHEN INSTR (fi.physician_service_name,'|',1,5) > 0
          THEN SUBSTR (fi.physician_service_name,INSTR (fi.physician_service_name,'|',1,4)+ 1,INSTR (fi.physician_service_name,'|', 1,5)- INSTR (fi.physician_service_name,'|',1,4)- 1)
          ELSE SUBSTR (fi.physician_service_name,INSTR (fi.physician_service_name,'|',1,4)+ 1,LENGTH (fi.physician_service_name)- INSTR (fi.physician_service_name,'|',1,4))
        END
      ELSE NULL
    END AS physician_service_name_5
  FROM 
  (
    SELECT 
      ep.network,
      ep.emp_provider_id AS provider_id,
      ep.provider_name,
      ep.title_id,
      ep.title_name,
      ep.title_prefix,
      ep.title_suffix,
      ep.physician_flag,
      ep.physician_group_id,
      ep.physician_group_name,
      ep.emp_id AS emp,
      ep.license_id AS license,
      ep.social_security_id AS social_security,
      ep.sdg_emp_no_id AS sdg_emp_no,
      ep.prac_npi_id AS prac_npi,
      ep.npi_id AS npi,
      ep.license_exp_date_id AS license_exp_date_id,
      LISTAGG(ms.physician_service_id,'|')WITHIN GROUP (ORDER BY ep.emp_provider_id,ms.physician_service_id)
        OVER(PARTITION BY ep.network,ep.emp_provider_id,ep.license_id) AS physician_service_id,
      LISTAGG(ms.name,'|') WITHIN GROUP (ORDER BY ep.emp_provider_id,ms.physician_service_id)
        OVER(PARTITION BY ep.network,ep.emp_provider_id,ep.license_id)AS physician_service_name,
      ROW_NUMBER() OVER (PARTITION BY ep.network, ep.emp_provider_id ORDER BY ep.license_id) AS rnk
    FROM prov_data ep
    LEFT JOIN emp_facility_med_spec efms ON ep.emp_provider_id = efms.emp_provider_id AND ep.network = efms.network
    LEFT JOIN medical_specialty ms ON efms.physician_service_id = ms.physician_service_id AND efms.facility_id = ms.facility_id AND efms.network = ms.network
  ) fi
  WHERE rnk = 1
) p
LEFT JOIN dim_providers d ON d.provider_id = p.provider_id AND d.network = p.network AND d.current_flag = 1;

CREATE OR REPLACE TRIGGER tr_v_dim_providers
INSTEAD OF INSERT ON v_dim_providers FOR EACH ROW
BEGIN
  IF :new.change <> 'NEW' THEN
    UPDATE dim_providers SET effective_to = TRUNC(SYSDATE), current_flag = 0
    WHERE ROWID = :new.row_id;
  END IF;

  INSERT INTO	dim_providers
  (
    provider_key, network, provider_id, archive_number, provider_name, title_id, title_name,
    title_prefix, title_suffix, physician_flag, emp, license, social_security, sdg_emp_no,
    prac_npi, npi, license_exp_date_id, physician_service_id, physician_service_name, physician_service_id_1, 
    physician_service_name_1, physician_service_id_2, physician_service_name_2, physician_service_id_3, physician_service_name_3,
    physician_service_id_4, physician_service_name_4, physician_service_id_5, physician_service_name_5, source,
    effective_from, effective_to, current_flag            
  )
  VALUES
  (
    seq_dim_providers.NEXTVAL,:new.network,:new.provider_id, NVL(:new.archive_number, 0) + 1,:new.provider_name, :new.title_id,
    :new.title_name, :new.title_prefix, :new.title_suffix, :new.physician_flag, :new.emp, :new.license,
    :new.social_security, :new.sdg_emp_no, :new.prac_npi, :new.npi, :new.license_exp_date_id, :new.physician_service_id,
    :new.physician_service_name, :new.physician_service_id_1, :new.physician_service_name_1, :new.physician_service_id_2,
    :new.physician_service_name_2, :new.physician_service_id_3, :new.physician_service_name_3, :new.physician_service_id_4,
    :new.physician_service_name_4, :new.physician_service_id_5, :new.physician_service_name_5, :new.source,  TRUNC(SYSDATE),  DATE '9999-12-31',  1
  );
END;
/  