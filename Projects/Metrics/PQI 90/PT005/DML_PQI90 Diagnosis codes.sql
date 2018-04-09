INSERT INTO meta_changes(comments) VALUES('Adding meta-data for the PQI90 reports  #1, #3, #5, #7, #8, #10, #11, #12, #14, #15 and #16');

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


INSERT INTO dsrip_reports VALUES
(
  'PQI90-5',
  'Prevension Quality Composit',
  'Patients aged 18 and older hospitalized in the last month',
  NULL,
  'Patients who were hospitalized with one of the "COPD or Asthma in older adults (40 years and above)" diagnoses',
  NULL,
  NULL,
  NULL,
  NULL
);

INSERT INTO dsrip_reports VALUES
(
  'PQI90-78',
  'Prevension Quality Composit',
  'Patients aged 18 and older hospitalized in the last month',
  NULL,
  'Patients who were hospitalized with one of the "Hypertension" diagnoses',
  'Patients who were hospitalized with one of the "Heart failure" diagnoses',
  NULL,
  NULL,
  NULL
);


INSERT INTO dsrip_reports VALUES
(
  'PQI90-10',
  'Prevension Quality Composit',
  'Patients aged 18 and older hospitalized in the last month',
  NULL,
  'Patients who were hospitalized with one of the "Dehydration" diagnoses',
  NULL,
  NULL,
  NULL,
  NULL
);

INSERT INTO dsrip_reports VALUES
(
  'PQI90-11',
  'Prevension Quality Composit',
  'Patients aged 18 and older hospitalized in the last month',
  NULL,
  'Patients who were hospitalized with one of the "Bacterial Pneumonia" diagnoses',
  NULL,
  NULL,
  NULL,
  NULL
);

INSERT INTO dsrip_reports VALUES
(
  'PQI90-12',
  'Prevension Quality Composit',
  'Patients aged 18 and older hospitalized in the last month',
  NULL,
  'Patients who were hospitalized with one of the "Urinary Tract Infection" diagnoses',
  NULL,
  NULL,
  NULL,
  NULL
);

INSERT INTO dsrip_reports VALUES
(
  'PQI90-15',
  'Prevension Quality Composit',
  'Patients aged 18 and older hospitalized in the last month',
  NULL,
  'Patients who were hospitalized with one of the "Asthma in Yound Adults" diagnoses',
  NULL,
  NULL,
  NULL,
  NULL
);

INSERT INTO dsrip_reports VALUES
(
  'PQI90-16',
  'Prevension Quality Composit',
  'Patients aged 18 and older hospitalized in the last month',
  NULL,
  'Lower-Extremity Amputation among Patients with Diabetes',
  NULL,
  NULL,
  NULL,
  NULL
);

INSERT INTO meta_criteria VALUES(50, 'DIAGNOSES:DIABETES SHORT TERM COMPLICATIONS:PQI90-1', 'List of Diabetes short term complications Diagnoses included into the Numerator of the report PQI90 #1');
INSERT INTO meta_criteria VALUES(51, 'DIAGNOSES:DIABETES LONG TERM COMPLICATIONS:PQI90-3', 'List of Diabetes long term complications included into the Numerator of the report PQI90 #3');
INSERT INTO meta_criteria VALUES(53, 'DIAGNOSES:(COPD) OR ASTHMA OLDER ADLTS ADMRATE:PQI90-5', 'List of COPD and ASTHMA Diagnoses included into the Numerator of the report PQI90 #5');

INSERT INTO meta_criteria VALUES(38, 'DIAGNOSES:HYPERTENSION:PQI90-7', 'List of Hypertension Diagnoses included into the Numerator of the report PQI90 #7');  
INSERT INTO meta_criteria VALUES(39, 'DIAGNOSES:HEART FAILURE:PQI90-8', 'List of Heart Failure Diagnoses included into the Numerator of the report PQI90 #8');  

INSERT INTO meta_criteria VALUES(54, 'DIAGNOSES:DEHYDRATION:PQI90-10', 'List of Dehydration Diagnoses included into the Numerator of the report PQI90 #10');
INSERT INTO meta_criteria VALUES(55, 'DIAGNOSES:BACTERIAL PNEUMONIA:PQI90-11', 'List of Bacterial Pneumonia Diagnoses included into the Numerator of the report PQI90 #11');
INSERT INTO meta_criteria VALUES(56, 'DIAGNOSES:URINARY TRACT INFECTION:PQI90-12', 'List of Urinary Tract Infection Diagnoses included into the Numerator of the report PQI90 #12');
INSERT INTO meta_criteria VALUES(52, 'DIAGNOSES:UNCONTROLLED DIABETES:PQI90-14', 'List of Uncontrolled Diabetes Diagnoses included into the Numerator of the report PQI90 #14');
INSERT INTO meta_criteria VALUES(57, 'DIAGNOSES:ASTHMA IN YOUND ADULTS:PQI90-15', 'List of Asthma in Yound Adults Diagnoses included into the Numerator of the report PQI90 #15');
INSERT INTO meta_criteria VALUES(58, 'DIAGNOSES:LOWER-EXTREMITY AMPUTATION WITH DIAB:PQI90-16', 'List of Lower-Extremity Amputation among Patients with Diabetes Diagnoses included into the Numerator of the report PQI90 #16');



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
SELECT 53, 'ALL', 'ICD10', t.COLUMN_VALUE, 'COPD or Asthma in older adults Diagnosis', 'DI', '=', 'I'
FROM TABLE
(
  tab_v256
  (
    'J410','J411','J418','J42','J430','J431','J432','J438','J439','J440','J441','J449',
    'J470','J471','J479','J4521','J4522','J4531','J4532','J4541','J4542','J4551','J4552',
    'J45901','J45902','J45990','J45991','J45998'
  )
)t;





INSERT INTO meta_conditions
SELECT 38, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Hypertension Diagnosis', 'DI', '=', 'I'
FROM TABLE(tab_v256('I10','I11.9','I12.9','I13.10')) t;

INSERT INTO meta_conditions
SELECT 39, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Heart Failure Diagnosis', 'DI', '=', 'I'
FROM TABLE
(
  tab_v256
  (
    'I09.81','I50.30','I11.0','I50.31','I13.0','I50.32','I13.2','I50.33',
    'I50.1','I50.40','I50.20','I50.41','I50.21','I50.42','I50.22','I50.43','I50.23','I50.9'
  )
) t;





INSERT INTO meta_conditions
SELECT 54, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Dehydration Diagnosis', 'DI', '=', 'I'
FROM TABLE
(
  tab_v256
  (
    'E860','E869','E861','E870','A080','A0839','A0811','A084','A0819','A088','A082','A09','A0831','K5289','A0832','K529','N170','N179','N171','N19','N172','N990','N178'
  )
)t;

INSERT INTO meta_conditions
SELECT 54, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Dehydration Diagnosis', 'DI', '=', 'E'
FROM TABLE(tab_v256('I120','N185','I1311','N186','I132'))t;




INSERT INTO meta_conditions
SELECT 55, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Bacterial Pneumonia Diagnosis', 'DI', '=', 'I'
FROM TABLE
(
  tab_v256
  (
    'J13','J159','J14','J160','J15211','J168','J15212','J180','J153','J181','J154','J188','J157','J189'
  )
)t;

INSERT INTO meta_conditions
SELECT 55, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Bacterial Pneumonia Diagnosis', 'DI', '=', 'E'
FROM TABLE
(
  tab_v256
  (
    'D5700','D5740','D5701','D57411','D5702','D57412','D571','D57419','D5720','D5780','D57211','D57811','D57212','D57812','D57219',
    'D57819','B20','I1311','B59','I132','C802','K912','C888','M359','C9440','N185','C9441','N186','C9442','T8600','C946','T8601','D4622',
    'T8602','D471','T8603','D479','T8609','D47Z1','T8610','D47Z9','T8611','D6109','T8612','D61810','T8613','D61811','T8619','D61818','T8620',
    'D700','T8621','D701','T8622','D702','T8623','D704','T86290','D708','T86298','D709','T8630','D71','T8631','D720','T8632','D72810','T8633',
    'D72818','T8639','D72819','T8640','D7381','T8641','D7581','T8642','D761','T8643','D762','T8649','D763','T865','D800','T86810','D801','T86811','D802',
    'T86812','D803','T86818','D804','T86819','D805','T86830','D806','T86831','D807','T86832','D808','T86838','D809','T86839','D810','T86850','D811','T86851',
    'D812','T86852','D814','T86858','D816','T86859','D817','T86890','D8189','T86891','D819','T86892','D820','T86898','D821','T86899','D822','T8690','D823','T8691',
    'D824','T8692','D828','T8693','D829','T8699','D830','Z4821','D831','Z4822','D832','Z4823','D838','Z4824','D839','Z48280','D840','Z48290','D841','Z48298',
    'D848','Z4901','D849','Z4902','D893','Z4931','D89810','Z940','D89811','Z941','D89812','Z942','D89813','Z943','D8982','Z944','D8989','Z9481','D899','Z9482',
    'E40','Z9483','E41','Z9484','E42','Z9489','E43','Z992','I120'
  )
)t;



INSERT INTO meta_conditions
SELECT 56, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Urinary Tract Infection Diagnosis', 'DI', '=', 'I'
FROM TABLE
(
  tab_v256
  (
    'N10','N2885','N119','N2886','N12','N3000','N151','N3001','N159','N3090','N16','N3091','N2884','N390'
  )
)t;

INSERT INTO meta_conditions
SELECT 56, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Urinary Tract Infection Diagnosis', 'DI', '=', 'E'
FROM TABLE
(
  tab_v256
  (
    'N110','Q6232','N111','Q6239','N118','Q624','Q625','N1370','Q6260','N1371','Q6261','N13721','Q6262','N13722',
    'Q6263','N13729','Q6269','N13731','Q627','N13732','Q628','N13739','Q630','N139','Q631','Q600','Q632','Q601','Q633',
    'Q602','Q638','Q603','Q639','Q604','Q6410','Q605','Q6411','Q606','Q6412','Q6100','Q6419','Q6101','Q642','Q6102','Q6431',
    'Q6111','Q6432','Q6119','Q6433','Q612','Q6439','Q613','Q645','Q614','Q646','Q615','Q6470','Q618','Q6471','Q619',
    'Q6472','Q620','Q6473','Q6210','Q6474','Q6211','Q6475','Q6212','Q6479','Q622','Q648','Q6231','Q649','B20','I1311','B59','I132',
    'C802','K912','C888','M359','C9440','N185','C9441','N186','C9442','T8600','C946','T8601','D4622','T8602','D471','T8603','D479','T8609',
    'D47Z1','T8610','D47Z9','T8611','D6109','T8612','D61810','T8613','D61811','T8619','D61818','T8620','D700','T8621','D701','T8622',
    'D702','T8623','D704','T86290','D708','T86298','D709','T8630','D71','T8631','D720','T8632','D72810','T8633','D72818','T8639','D72819','T8640',
    'D7381','T8641','D7581','T8642','D761','T8643','D762','T8649','D763','T865','D800','T86810','D801','T86811','D802','T86812','D803','T86818',
    'D804','T86819','D805','T86830','D806','T86831','D807','T86832','D808','T86838','D809','T86839','D810','T86850','D811','T86851','D812',
    'T86852','D814','T86858','D816','T86859','D817','T86890','D8189','T86891','D819','T86892','D820','T86898','D821','T86899','D822','T8690',
    'D823','T8691','D824','T8692','D828','T8693','D829','T8699','D830','Z4821','D831','Z4822','D832','Z4823','D838','Z4824','D839','Z48280','D840','Z48290','D841',
    'Z48298','D848','Z4901','D849','Z4902','D893','Z4931','D89810','Z940','D89811','Z941','D89812','Z942','D89813','Z943','D8982','Z944','D8989','Z9481','D899',
    'Z9482','E40','Z9483','E41','Z9484','E42','Z9489','E43','Z992','I120'
  )
)t;


INSERT INTO meta_conditions
SELECT 52, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Uncontrolled Diabetes', 'DI', '=', 'I'
FROM TABLE(tab_v256('E1065','E10649','E1165','E11649'))t;
  




INSERT INTO meta_conditions
SELECT 57, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Asthma in Yound Adults Diagnosis', 'DI', '=', 'I'
FROM TABLE
(
  tab_v256
  (
   'J4521','J4522','J4531','J4532','J4541','J4542','J4551','J4552','J45901','J45902','J45990','J45991','J45998'
  )
)t;

INSERT INTO meta_conditions
SELECT 57, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Asthma in Yound Adults Diagnosis', 'DI', '=', 'E'
FROM TABLE
(
  tab_v256
  (
    'E840','E8411','E8419','E848','E849','J8483','J84841','J84842','J84843','J84848','P270','P271','P278','P279',
    'Q254','Q311','Q312','Q313','Q315','Q318','Q319','Q320','Q321','Q322','Q323','Q324','Q330','Q331','Q332','Q333',
    'Q334','Q335','Q336','Q338','Q339','Q340','Q341','Q348','Q349','Q390','Q391','Q392','Q393','Q394','Q893'
  )
)t;

INSERT INTO meta_conditions
SELECT 58, 'ALL', 'ICD10', t.COLUMN_VALUE, 'Lower-Extremity Amputation among Patients with Diabetes Diagnosis', 'DI', '=', 'I'
FROM TABLE
(
  tab_v256
  (
    'E1010','E1144','E1011','E1149','E1021','E1151','E1022','E1152','E1029','E1159','E10311','E11610','E10319','E11618','E10321',
    'E11620','E10329','E11621','E10331','E11622','E10339','E11628','E10341','E11630','E10349','E11638','E10351','E11641','E10359',
    'E11649','E1036','E1165','E1039','E1169','E1040','E118','E1041','E119','E1042','E1300','E1043','E1301','E1044','E1310','E1049',
    'E1311','E1051','E1321','E1052','E1322','E1059','E1329','E10610','E13311','E10618','E13319','E10620','E13321','E10621',
    'E13329','E10622','E13331','E10628','E13339','E10630','E13341','E10638','E13349','E10641','E13351','E10649','E13359','E1065','E1336',
    'E1069','E1339','E108','E1340','E109','E1341','E1100','E1342','E1101','E1343','E1121','E1344','E1122','E1349','E1129','E1351','E11311',
    'E1352','E11319','E1359','E11321','E13610','E11329','E13618','E11331','E13620','E11339','E13621','E11341','E13622','E11349','E13628',
    'E11351','E13630','E11359','E13638','E1136','E13641','E1139','E13649','E1140','E1365','E1141','E1369','E1142','E138','E1143','E139'
  )
)t;

COMMIT;
