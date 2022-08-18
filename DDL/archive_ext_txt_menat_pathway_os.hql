-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create archive table for 
---           IMS Stock data
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`archive_ext_txt_menat_pathway_os`(
`year` int,
`period` string,
`distributorcode` string,
`dfucode` string,
`dfudescription` string,
`kelloggbrand` string,
`volume` float,
`load_date` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_ksop/OS/archive'
TBLPROPERTIES (
'skip.header.line.count'='1')