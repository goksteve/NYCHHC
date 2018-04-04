 --execute cdw.sp_sg_refresh_metric_results;

select * from DBG_LOG_DATA
where proc_id  =  260
order by tstamp desc;

