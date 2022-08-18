-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 3
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---            SUPPLY MODULE Data (SUPPLY MODULE REPORT).
-------------------------------------------------------------------------------
CREATE TABLE amea.pcd_ext_orc_sap_supply_module
(
Brand  string
,DFU string
,DFUnumber string
,Category string
,Orders double
,Actual_Shipped double
,VAR double
,Year int
,Period int
,Load_date timestamp
)
PARTITIONED BY(
Report_Year	int
,Report_Period int
,Country string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/work/kap/menat_ksop/supply_module/pcd_ext_orc_sap_supply_module'



