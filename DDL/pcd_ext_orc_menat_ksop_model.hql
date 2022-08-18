-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 3
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           KSOP Model Data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE `amea.pcd_ext_orc_menat_ksop_model`(
`country` string,
`country_group` string,
`category` string,
`brand` string,
`dfu` string,
`ims` double,
`exf` double,
`arrivals` double,
`closing_stock` double,
`ryear` string,
`rmonth` string,
`dfu_description` string,
`stock_days` double,
`load_date` timestamp)
PARTITIONED BY (
`distributor` string,
`year` int,
`period` string,
`version` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION
'/work/kap/menat_ksop/ksop_model/pcd_ext_orc_menat_ksop_model'
