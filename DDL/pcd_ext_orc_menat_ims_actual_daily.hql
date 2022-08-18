-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           IMS Actuals daily data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`pcd_ext_orc_menat_ims_actual_daily`(
`kelloggskucode` string,
`kelloggskudescription` string,
`kelloggbrand` string,
`packsizegrams` float,
`caseweightgrams` float,
`promo_non_promo` string,
`channel` string,
`subchannel` string,
`salesincases` float,
`distributorgsv` string,
`load_date` timestamp,
`delta_salesincases` float)
PARTITIONED BY (
`year` int,
`period` string,
`distributorcode` string,
`short_load_date` date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/work/kap/menat_dsr/ims_actual_daily/pcd'