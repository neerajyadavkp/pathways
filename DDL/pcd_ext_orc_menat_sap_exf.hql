-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 3
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           SAP EXF Data (KSOP Model Report).
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE
amea.pcd_ext_orc_menat_sap_exf
(report_period int
,dfu string
,material string
,materialdesc string
,plantcode int
,billto int
,market string
,transittime string
,shipmentperiod int
,year int
,cases double
,timeframe int
,load_date timestamp
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/work/kap/menat_ksop/sap_exf/pcd'