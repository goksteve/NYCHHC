INSERT INTO meta_changes  ( comments) values('SG added new criteria');
--*** TR44 **----------

--85	DIAGNOSES:MYALGIA, MYOSITIS, MYOPATHY
--84	DIAGNOSES:CIRRHOSIS ICD CODES
--83	DIAGNOSES:END-STAGE RENAL DISEASES

INSERT INTO meta_criteria
SELECT
 MAX(criterion_id) + 1, 'DIAGNOSES:END-STAGE RENAL DISEASES', 'List of end-stage Renal Diseases ICD Codes'
FROM
 meta_criteria;

INSERT INTO meta_criteria
SELECT
 MAX(criterion_id) + 1, 'DIAGNOSES:CIRRHOSIS ICD CODES', 'List of Cirrhosis ICD Codes'
FROM
 meta_criteria;

INSERT INTO meta_criteria
SELECT
 MAX(criterion_id) + 1,
 'DIAGNOSES:MYALGIA, MYOSITIS, MYOPATHY',
 'List of Myalgia, myositis, myopathy,or rhabdomyolysis ICD Codes'
FROM
 meta_criteria;

--**** CLOMIFENE TREATS INFERTILITY IN WOMEN 
INSERT INTO meta_criteria
SELECT
 MAX(criterion_id) + 1, 'MEDICATIONS:CLOMIFENE TREATS INFERTILITY IN WOMEN', 'List of Clomifene medications '
FROM
 meta_criteria;
