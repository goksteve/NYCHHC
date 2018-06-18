/* Formatted on 3/30/2018 12:57:46 PM (QP5 v5.287) */
--DROP TABLE TR021_ADHD_STG_1 PURGE;
--
--
--CREATE /*+ PARALLEL(32) */
--      TABLE TR021_ADHD_STG_1
--NOLOGGING
--AS
   WITH Z_RPT
        AS (SELECT TRUNC (NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE)) RPT_RUN_DT,
                   ADD_MONTHS (TRUNC (NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH'), -24) - 120 AS LMT_DT,
                   ADD_MONTHS (TRUNC (NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE), 'MONTH'), -24) AS RPT_STRT_DT,
                   LAST_DAY (ADD_MONTHS (TRUNC (NVL(TO_DATE(SYS_CONTEXT('USERENV', 'CLIENT_IDENTIFIER')), SYSDATE)), -1)) AS RPT_END_DT
              FROM DUAL)
     SELECT p.*,
            FIRST_VALUE (ORDER_TIME)
               OVER (PARTITION BY patient_id ORDER BY NULL)
               MODIFIED_PROC_NAME,
            MAIN_BLOCK_DISPLAY,
            RPT_STRT_DT,
            RPT_END_DT,
            LMT_DT,
            RPT_RUN_DT
       -- MODIFIED_PROC_NAME

       FROM prescription p
            LEFT JOIN order_span os
               ON     p.network = os.network
                  AND p.order_visit_id = os.visit_id
                  AND p.order_span_id = os.order_span_id
                  AND p.proc_id = os.main_proc_id,
            Z_RPT
      WHERE     (TRUNC (ORDER_TIME) BETWEEN LMT_DT AND RPT_END_DT)
            AND                                     --patient_id = 1833772 AND
                (   REGEXP_LIKE (
                       MISC_NAME,
                       'Adderall|Amphetamine-Dextroamphetamine|Aptensio XR|atomoxetine|Catapres|cloNIDine|Concerta|Daytrana|Desoxyn|Dexedrine Spansule|dexmethylphenidate|dextroamphetamine|Duraclon|Focalin|guanFACINE|Intuniv|Kapvay|Liquadd|lisdexamfetamine|Metadate CD|Metadate ER|methamphetamine|Methylin|methylphenidate|Mydayis|ProCentra|QuilliChew ER|Quillivant XR|Ritalin|Strattera|Tenex|Vyvanse|Zenzedi',
                       'i')
                 OR REGEXP_LIKE (
                       MAIN_BLOCK_DISPLAY,
                       'Adderall|Amphetamine-Dextroamphetamine|Aptensio XR|atomoxetine|Catapres|cloNIDine|Concerta|Daytrana|Desoxyn|Dexedrine Spansule|dexmethylphenidate|dextroamphetamine|Duraclon|Focalin|guanFACINE|Intuniv|Kapvay|Liquadd|lisdexamfetamine|Metadate CD|Metadate ER|methamphetamine|Methylin|methylphenidate|Mydayis|ProCentra|QuilliChew ER|Quillivant XR|Ritalin|Strattera|Tenex|Vyvanse|Zenzedi',
                       'i'))
   ORDER BY order_time;

DROP TABLE TR021_ADHD_STG_2 PURGE;

CREATE /*+ PARALLEL(32) */
      TABLE TR021_ADHD_STG_2
NOLOGGING
AS
     SELECT Network, 
            PATIENT_ID,
            ADHD_MED_RX_DATE,
            LEAD (ADHD_MED_RX_DATE, 1)
               OVER (PARTITION BY Network, PATIENT_ID ORDER BY ADHD_MED_RX_DATE)
               follow_up_date,
            (  LEAD (ADHD_MED_RX_DATE, 1)
                  OVER (PARTITION BY Network, PATIENT_ID ORDER BY ADHD_MED_RX_DATE)
             - ADHD_MED_RX_DATE)
               Number_of_Days,
            ERLST_ADHD_MED_RX_DATE,
            ADHD_RX_MEDICATION,
            RPT_STRT_DT,
            RPT_END_DT
       FROM (  SELECT Network,                                             --DISTINCT
                     PATIENT_ID,
                      ADHD_MED_RX_DATE,
                      ERLST_ADHD_MED_RX_DATE,
                      --LEAD(ADHD_MED_RX_DATE,1) OVER(PARTITION BY PATIENT_ID ORDER BY NULL),
                      LISTAGG (ADHD_MED_DESC, CHR (10))
                         WITHIN GROUP (ORDER BY ADHD_MED_RX_DATE)
                         OVER (PARTITION BY Network, patient_id, ADHD_MED_RX_DATE)
                         ADHD_RX_MEDICATION,
                      RPT_STRT_DT,
                      RPT_END_DT,
                      ROW_NUMBER ()
                      OVER (PARTITION BY Network, patient_id, ADHD_MED_RX_DATE
                            ORDER BY NULL)
                         rk
                 FROM (  SELECT                                            --*
                                --DISTINCT
                                Network, 
                                p.patient_id,
                                rx_id,
                                TRUNC (order_time) ADHD_MED_RX_DATE,
                                FIRST_VALUE (TRUNC (order_time))
                                   OVER (PARTITION BY Network, patient_id ORDER BY NULL)
                                   ERLST_ADHD_MED_RX_DATE,
                                ROW_NUMBER ()
                                OVER (
                                   PARTITION BY Network, patient_id,
                                                TRUNC (order_time),
                                                MAIN_BLOCK_DISPLAY
                                   ORDER BY NULL)
                                   RN,
                                NVL(MISC_NAME,MAIN_BLOCK_DISPLAY) ADHD_MED_DESC,
                                RPT_STRT_DT,
                                RPT_END_DT
                           FROM TR021_ADHD_STG_1 p
                          WHERE                     --patient_id = 1858315 AND
                                TRUNC (ORDER_TIME) >= RPT_STRT_DT
                       ORDER BY Network, PATIENT_ID, ORDER_TIME ASC) Z
                WHERE rn = 1
             ORDER BY Network, PATIENT_ID, ADHD_MED_RX_DATE ASC) yy
      WHERE rk = 1
   ORDER BY Network, PATIENT_ID, ADHD_MED_RX_DATE ASC;
   

DROP table tr021_adhd_rx_prescription purge;

create /*+ PARALLEL(32) */
      table tr021_adhd_rx_prescription
nologging
as
/* Formatted on 4/24/2018 2:27:56 PM (QP5 v5.287) */
Select
network               ,
patient_id            ,
mrn                   ,
patient_name          ,
date_of_birth         ,
age_on_rx_dt          ,
streetadr             ,
apt_suite             ,
city                  ,
state                 ,
zipcode               ,
country               ,
home_phone            ,
adhd_med_rx_date      ,
follow_up_date        ,
number_of_days        ,
erlst_adhd_med_rx_date,
adhd_rx_medication    ,
numerator             ,
'QCPR' as source      ,
rpt_strt_dt           ,
rpt_end_dt            ,
sysdate as load_date
from
(
select v.*,
       case
          when mdm.onmlast is not null and mdm.onmfirst is not null
          then
             mdm.onmlast || ', ' || mdm.onmfirst
       end
          patient_name,
       round ( (adhd_med_rx_date - to_date (mdm.dob, 'YYYY-MM-DD')) / 365.25,
              0)
          as "AGE_ON_RX_DT",
       to_date (mdm.dob, 'YYYY-MM-DD') date_of_birth,
       case when number_of_days <= 30 then 1 end as numerator,
       mdm.mrn,
       mdm.streetadr,
       mdm.apt_suite,
       mdm.city,
       mdm.state,
       mdm.zipcode,
       mdm.country,
       nvl (mdm.home_phone, mdm.day_phone) home_phone
  from tr021_adhd_stg_2 v
       left join 
           (select mdm.*, row_number ()
            over (partition by network, patientid order by dc_flag nulls last) rn
            from dconv.mdm_qcpr_pt_02122016 mdm
            where mdm.epic_flag = 'N'
           ) mdm
             on     mdm.network = v.network
             and to_number (mdm.patientid) = v.patient_id
             and mdm.rn=1
--             and mdm.epic_flag = 'N'                 --AND mdm.dc_flag IS NULL
 where round ( (adhd_med_rx_date - to_date (mdm.dob, 'YYYY-MM-DD')) / 365.25,
              0) between 6
                     and 12
);  


Select * from tr021_adhd_rx_prescription;


Select 	count(*) dnmr , 
		count(numerator) nmr, 
		round((count(numerator)/count(*))*100,0)||'%' pct ,
		network 
		from tr021_adhd_rx_prescription 
		group by network ;
