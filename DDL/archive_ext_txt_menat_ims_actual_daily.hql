-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create archive table for 
---           IMS Actuals daily data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`archive_ext_txt_menat_ims_actual_daily`(
`distributorcode` string,
`period` string,
`year` int,
`kelloggskucode` string,
`kelloggskudescription` string,
`kelloggbrand` string,
`packsize` float,
`caseweight` float,
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
LOCATION '/work/kap/menat_dsr/ims_actual_daily/archive'
TBLPROPERTIES (
'skip.header.line.count'='1'
)