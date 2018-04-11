INSERT INTO meta_changes(comments) VALUES('Adding meta-data for the PQI90 reports #1, #3 and #14');

INSERT INTO dsrip_reports VALUES
(
  'PQI90-1_3_14',
  'Prevension Quality Composit',
  'Patients aged 18 and older hospitalized in the last month',
  NULL,
  'Patients who were hospitalized with one of the "Diabetes short term complications" diagnoses',
  'Patients who were hospitalized with one of the "Diabetes long term complications" diagnoses',
  'Uncontrolled Diabetes',
  NULL,
  NULL
);

INSERT INTO meta_criteria VALUES(50, 'DIAGNOSES:DIABETES SHORT TERM COMPLICATIONS:PQI90-1', 'List of Diabetes short term complications Diagnoses included into the Numerator of the report PQI90 #1');
INSERT INTO meta_criteria VALUES(51, 'DIAGNOSES:DIABETES LONG TERM COMPLICATIONS:PQI90-3', 'List of Diabetes long term complications included into the Numerator of the report PQI90 #3');
INSERT INTO meta_criteria VALUES(52, 'DIAGNOSES:UNCONTROLLED DIABETES:PQI90-14', 'List of Uncontrolled Diabetes Diagnoses included into the Numerator of the report PQI90 #14');



INSERT INTO meta_conditions
SELECT 50, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Diabetes short term complications', 'DI', '=', 'I'
FROM TABLE(tab_v256('E1010','E1100','E1011','E1101','E10641','E11641','E1065','E1165')) t;



INSERT INTO meta_conditions
SELECT 51, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Diabetes long term complications', 'DI', '=', 'I'
FROM TABLE
(
  tab_v256
  (
    'E1021','E1121','E1022','E1122','E1029','E1129','E10311','E11311','E10319','E11319','E10321','E11321','E10329','E11329','E10331',
    'E11331','E10339','E11339','E10341','E11341','E10349','E11349','E10351','E11351','E10359','E11359','E1036','E1136','E1039','E1139',
    'E1040','E1140','E1041','E1141','E1042','E1142','E1043','E1143','E1044','E1144','E1049','E1149','E1051','E1151','E1052','E1152','E1059',
    'E1159','E10610','E11610','E10618','E11618','E10620','E11620','E10621','E11621','E10622','E11622','E10628','E11628',
    'E10630','E11630','E10638','E11638','E1069','E1169','E108','E118'
  )
)t;


INSERT INTO meta_conditions
SELECT 52, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Uncontrolled Diabetes', 'DI', '=', 'I'
FROM TABLE(tab_v256('E1065','E10649','E1165','E11649'))t;
  
COMMIT;
