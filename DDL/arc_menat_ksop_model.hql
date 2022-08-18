CREATE EXTERNAL TABLE `amea.arc_menat_ksop_model`(
`country` string,
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
LOCATION
'/work/kap/menat_ksop/ksop_model/arc_menat_ksop_model'
