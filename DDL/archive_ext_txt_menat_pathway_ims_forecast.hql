-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create archive table for 
---           IMS Forecast data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`archive_ext_txt_menat_pathway_ims_forecast`(
`year` int,
`period` string,
`distributorcode` string,
`dfucode` string,
`dfudescription` string,
`kelloggbrand` string,
`packsize` float,
`caseweight` float,
`promo_nonpromo` string,
`channel` string,
`subchannel` string,
`yearperiod` string,
`sales` float,
`version` string,
`load_date` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_ksop/ims_forecast/archive'
TBLPROPERTIES (
'skip.header.line.count'='1'
)
