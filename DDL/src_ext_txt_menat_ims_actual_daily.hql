-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create src table for 
---           IMS Actuals daily data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`src_ext_txt_menat_ims_actual_daily`(
`distributorcode` string,
`period` string,
`year` int,
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
`load_date` string,
`short_load_date` string,
`delta_salesincases` float)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_dsr/ims_actual_daily/src'
TBLPROPERTIES (
'skip.header.line.count'='1'
)