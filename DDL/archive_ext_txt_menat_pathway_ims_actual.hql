-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create archive table for 
---           IMS Actuals data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`archive_ext_txt_menat_pathway_ims_actual`(
`distributorcode` string,
`period` string,
`year` int,
`dfucode` string,
`dfudescription` string,
`kelloggbrand` string,
`packsize` float,
`caseweight` float,
`promo_non_promo` string,
`channel` string,
`subchannel` string,
`sales` float,
`distributorgsv` string,
`load_date` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_ksop/ims_actual/archive'
TBLPROPERTIES (
'skip.header.line.count'='1'
)