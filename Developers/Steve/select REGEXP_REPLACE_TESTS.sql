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


Select regexp_replace( trim('52.047.0000 ... 14/25/2017.at ho.m.e...?.?1+_  '),  '[-?!.+_]+$','') from dual;


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


  select SUBSTR(
     REGEXP_REPLACE(
      REGEXP_REPLACE(
       REGEXP_REPLACE(
        REGEXP_REPLACE(
         REGEXP_REPLACE(REGEXP_REPLACE('99 7 Oct 16', '[-?!.+_]+$', ''), '[[:alpha:]]\.'),
         '[\)(]'),
        '([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})'),
       '[^[:digit:]. ]'),
      '\.$'),
     1,
     3) FROM dual


select REGEXP_REPLACE('99  7 Oct 16', '[-?!.+_]+$', '') from dual.

  select REGEXP_REPLACE(
         REGEXP_REPLACE(REGEXP_REPLACE('99  7 Oct 16', '[-?!.+_]+$', ''), '[[:alpha:]]\.'),
         '[\)(]') from dual;

  select REGEXP_REPLACE(
        REGEXP_REPLACE(
         REGEXP_REPLACE(REGEXP_REPLACE('99  7 Oct 16', '[-?!.+_]+$', ''), '[[:alpha:]]\.'),
         '[\)(]'),
        '([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})') from dual

 select REGEXP_REPLACE(
       REGEXP_REPLACE(
        REGEXP_REPLACE(
         REGEXP_REPLACE(REGEXP_REPLACE('99  7 Oct 16', '[-?!.+_]+$', ''), '[[:alpha:]]\.'),
         '[\)(]'),
        '([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})'),
       '[^[:digit:].]') from dual

select regexp_replace ('precision xceed pro', ' ','') from dual
select  REGEXP_REPLACE(REGEXP_REPLACE(TRIM('ps1y ER'), '[-\=/.+,?><$*_ ]'), '[[:alpha:]\/.+-,''?>]', '-1') from dual;


SELECT REGEXP_REPLACE(TRIM(r.result_value), '[-\=/.+,?><$*_ ]') FROM DUAL,

SELECT
 1
FROM
 DUAL
WHERE
 REGEXP_REPLACE(TRIM('000'), '[[:alpha:]\/.+-,''?>)( 0]') IS NOT NULL



select 
  SUBSTR(TRIM(
     REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
       REGEXP_REPLACE(
        REGEXP_REPLACE(REGEXP_REPLACE(' 1.445', '[-?!.+_]+$', ''), '[[:alpha:]]\.'),
        '[\)(]'),
       '([[:digit:]]{1,2})\/([[:digit:]]{1,2})\/([[:digit:]]{2,4})'), 
'([[:digit:]]{4})'),
      '[^[:digit:]. ]')),
     1,
     5) from dual;

select  REGEXP_REPLACE( ' 05/05/2017 4121 q5 hylitil8r58o7 :', '([[:digit:]]{4})') from dual;

select REGEXP_REPLACE(TRIM('1./1'), '[[:alpha:]\/.+-,''?>)( 0]') from dual;


 -- AND       REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(r.result_value), '[-\=/.+,?!><$*_ @ )(]'), '[[:alpha:]]', '-1'),'-1') is NOT null;

select 1
from dual
where 
       REGEXP_REPLACE(SUBSTR( TRIM('.*rrrrrr.-//popp!@  #$  #%^   &&*+('),1,1), '[-\=/.+ ,?!><$*#^@%)(0&]') IS NOT NULL


