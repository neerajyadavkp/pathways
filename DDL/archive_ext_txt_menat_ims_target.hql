-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create archive table for 
---           IMS Target data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`archive_ext_txt_menat_ims_target`(
`year` int,
`period` string,
`kelloggbrand` string,
`distributorcode` string,
`volume` float,
`load_date` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_dsr/ims_target/archive'
TBLPROPERTIES (
'skip.header.line.count'='1'
)