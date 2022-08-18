-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           IMS Target data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`pcd_ext_orc_menat_ims_target`(
`kelloggbrand` string,
`volume` float,
`load_date` timestamp)
PARTITIONED BY (
`year` int,
`period` string,
`distributorcode` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/work/kap/menat_dsr/ims_target/pcd'