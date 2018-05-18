
UPDATE /*+ PARALLEL(48) */ log_incremental_data_load  set max_cid = ( SELECT  MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn))
where table_name = 'FACT_RESULTS' and NETWORK  = 'CBN'


(
SELECT /*+ PARALLEL(32) */
 lg.network,
 lg.table_name,
 lg.max_cid,
 res.res_cid
FROM
 log_incremental_data_load lg
 JOIN 
    (
      SELECT
      network, MAX(cid) AS res_cid
      FROM
      fact_results
      GROUP BY
      network
     ) res
  ON res.network = lg.network
WHERE
 table_name = 'FACT_RESULTS')
set  max_cid  =  res_cid ;




 UPDATE
   log_incremental_data_load
  SET
   max_cid = 0 
  WHERE
   table_name = 'FACT_RESULTS';

SELECT /*+ PARALLEL(32) */ MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn) ;
SELECT /*+ PARALLEL(32) */ MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn) ;
SELECT /*+ PARALLEL(32) */ MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn) ;
SELECT /*+ PARALLEL(32) */ MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn) ;
SELECT /*+ PARALLEL(32) */ MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn) ;
SELECT /*+ PARALLEL(32) */ MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn) ;
SELECT /*+ PARALLEL(32) */ MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn) ;
SELECT /*+ PARALLEL(32) */ MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn) ;
SELECT /*+ PARALLEL(32) */ MAX(cid) AS max_cid  FROM fact_results PARTITION(cbn) ;




UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  set max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) where table_name = 'FACT_RESULTS' AND NETWORK  = 'CBN' ;
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  set max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) where table_name = 'FACT_RESULTS' AND NETWORK  = 'GP1' ;
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  set max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) where table_name = 'FACT_RESULTS' AND NETWORK  = 'GP2' ;
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  set max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) where table_name = 'FACT_RESULTS' AND NETWORK  = 'NBN' ;
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  set max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) where table_name = 'FACT_RESULTS' AND NETWORK  = 'NBX' ;
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  set max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) where table_name = 'FACT_RESULTS' AND NETWORK  = 'QHN' ;
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  set max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) where table_name = 'FACT_RESULTS' AND NETWORK  = 'SBN' ;
UPDATE /*+ PARALLEL(32) */ log_incremental_data_load  set max_cid = (SELECT MAX(cid) AS max_cid FROM fact_results PARTITION(cbn)) where table_name = 'FACT_RESULTS' AND NETWORK  = 'SMN' ;
