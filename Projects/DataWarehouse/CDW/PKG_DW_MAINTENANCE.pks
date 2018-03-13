CREATE OR REPLACE PACKAGE pkg_dw_maintenance AS
/*
  Procedures for Data Warehouse maintenance
   
  Change history
  ------------------------------------------------------------------------------
  06-MAR-2018, OK: added procedure REFRESH_INCREMENTAL;
  16-FEB-2018, OK added procedures INIT_MAX_CIDS and RECORD_MAX_CIDS;
  01-FEB-2018, OK: created
*/
  TYPE typ_num_list IS TABLE OF NUMBER INDEX BY VARCHAR2(30);
  
  max_cids typ_num_list;
  
  PROCEDURE init_max_cids(p_table_name IN VARCHAR2); -- initializes MAX_CIDS array; 
  
  PROCEDURE record_max_cids(p_table_name IN VARCHAR2); -- saves new MAX_CIDS values in the table ETL_MAX_CIDS;

  PROCEDURE refresh_data(p_condition IN VARCHAR2 DEFAULT NULL);
  
  PROCEDURE refresh_full;
  
  PROCEDURE refresh_incremental;
END;
/

CREATE OR REPLACE SYNONYM dwm FOR pkg_dw_maintenance;
