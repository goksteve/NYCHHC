CREATE OR REPLACE VIEW v_dim_patients_stg AS
SELECT * FROM dim_patients
WHERE current_flag = 1;

CREATE OR REPLACE TRIGGER tr_v_dim_patients_stg 
INSTEAD OF INSERT OR UPDATE ON v_dim_patients_stg FOR EACH ROW
BEGIN
  WHEN INSERTING THEN
  NULL;
END;
/
