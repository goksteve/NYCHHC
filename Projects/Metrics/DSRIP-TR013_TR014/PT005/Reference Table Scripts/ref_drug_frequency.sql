CREATE TABLE ganesh_ref_drug_frequency
(
  drug_frequency VARCHAR2(512 BYTE),
  drug_frequency_num_val NUMBER(6),
  CONSTRAINT pk_ref_drug_frequency PRIMARY KEY(drug_frequency,drug_frequency_num_val)
);

SET DEFINE OFF;
--SQL Statement which produced this data:
--
--  SELECT 
--     ROWID, DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL
--  FROM PT005.GANESH_REF_DRUG_FREQUENCY;
--
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%2%times%day%', 2);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%at%bedtime%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%bedtime%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%bid%', 2);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%daily%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%evening%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%12%hour%', 2);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%6%hour%', 4);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%8%hour%', 3);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%afternoon%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%eight%hour%', 3);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%evening%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%morning%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%night%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%six%hour%', 4);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%every%twelve%hour%', 2);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%four%times%', 4);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%once%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%once%a%day%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%q%am%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%q%bedtime%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%q%pm%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%q12%', 2);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%q6%', 4);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%q8%', 3);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%qid%', 4);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%single%dose%', 1);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%three%times%day%', 3);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%tid%', 3);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%twice%', 2);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%two%times%', 2);
Insert into GANESH_REF_DRUG_FREQUENCY
   (DRUG_FREQUENCY, DRUG_FREQUENCY_NUM_VAL)
 Values
   ('%two%times%day%', 2);
COMMIT;


with q1
as
(select distinct frequency from GANESH_TST_FREQUENCY)
select q1.frequency,drug_frequency_num_val
from q1
left join ganesh_ref_drug_frequency q2 on lower(q1.frequency) like q2.drug_frequency;