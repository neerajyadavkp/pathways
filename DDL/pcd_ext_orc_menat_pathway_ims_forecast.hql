-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           IMS Forecast data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`pcd_ext_orc_menat_pathway_ims_forecast`(
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
`load_date` timestamp)
PARTITIONED BY (
`year` int,
`period` string,
`distributorcode` string,
`version` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/work/kap/menat_ksop/ims_forecast/pcd'
