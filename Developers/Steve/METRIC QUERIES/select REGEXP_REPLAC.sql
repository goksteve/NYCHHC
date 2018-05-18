select REGEXP_REPLACE('a/12458.rrt78_*-', '[^[:digit:].]') from dual 
union all
select REGEXP_REPLACE(REGEXP_REPLACE('516  repeat 497', '[^[:digit:].]'), '\.$')from dual;

SELECT
 REGEXP_REPLACE( REGEXP_REPLACE('57  12/27/2017 2257', '[\)(]')
  ,
  '([[:digit:]]{1,2})/([[:digit:]]{1,2})/([[:digit:]]{2,4})')
FROM
 DUAL;


select 'HH', REGEXP_replace ('n', '[[:alpha:]\.+-,''?]', '-1')
FROM DUAL 
WHERE REGEXP_replace (TRIM('n/01swerft   '), '[[:alpha:]\/.+-,''?>]') <> '0';

select TO_NUMBER(SUBSTR ( REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE ('57.0','[\)(]'),'([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})'), '[^[:digit:].]'), 1, 5)) from dual;
 select       TO_NUMBER(SUBSTR ( REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE('9.5', '[\)(]'),'([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})'), '[^[:digit:].]'), '\.$'), 1, 5))ss from dual

select max(9.5) from dual

select str_to_number ('00') from dual

SELECT
  REGEXP_REPLACE('ads.', 's.')
FROM
 DUAL;

select REGEXP_substr('2.0 blood sugar 05/4589 in lab. 122 mg/dl.', '\.') from dual;
select REGEXP_REPLACE(q.result_value, '[[:alpha:]]\.') from dual
select REGEXP_REPLACE(q.result_value, '[[:alpha:]]\.') from dual
select regexp_replace('52.047.00 14/25/2017.at home',  '.','', length('52.047.00 14/25/2017.at home')) from dual



select to_number


with T as
(
select 'some symbols   ab cd  lot of occurences of ab here  this is last ab occurence: ab  some more symbols ' s from dual
)   
 select regexp_replace(T.s,'ab','ab_last',instr(T.s,'ab',-1)) from t;


Select regexp_replace('52.047.0000 ... 14/25/2017.at home?...?',  '\.$','') from dual;


WITH 
tmp
AS
(
SELECT '00000'  g from dual
union
select '1' from dual 
)
select g
from tmp
WHERE g = '' 
   OR g NOT LIKE '%[^0]%'

select to_number ( '0.3247') from dual