-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 3
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           HISTORICAL IMS Data(KSOP REPORT).
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE
amea.pcd_ext_orc_menat_ksop_report_historical_ims
(Market String
,Distributor string
,Sold_To_Party string
,NEW_DFU string
,NEW_DESC string
,BRAND string
,Month int
,IMS float
,Load_Date timestamp
)
PARTITIONED BY (Year int
,Version string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/work/kap/menat_ksop/ksop_report/pcd_ext_orc_menat_ksop_report_historical_ims';