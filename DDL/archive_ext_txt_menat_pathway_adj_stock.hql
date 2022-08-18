-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create archive table for 
---           adjustment data
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`archive_ext_txt_menat_pathway_adj_stock`(
`year` int,
`period` string,
`distributorcode` string,
`dfucode` string,
`volume` float,
`load_date` timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_ksop/adj_stock/archive'
TBLPROPERTIES (
'skip.header.line.count'='1')