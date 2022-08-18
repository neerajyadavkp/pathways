-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           adjustment data
-------------------------------------------------------------------------------

CREATE EXTERNAL TABLE amea.`pcd_ext_orc_menat_pathway_adj_stock`(
`dfucode` string,
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
LOCATION '/work/kap/menat_ksop/adj_stock/pcd'