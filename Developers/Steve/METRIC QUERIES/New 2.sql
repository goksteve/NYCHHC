create table stev_DEL_TEST
as
select /*+ parallel(32)*/ * from steve_del
where criterion_id  <> 13

select  1 from dual 
 where '6.9' > '1000'