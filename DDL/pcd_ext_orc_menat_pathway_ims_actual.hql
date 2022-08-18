-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           IMS Actuals data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`pcd_ext_orc_menat_pathway_ims_actual`(
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
`load_date` timestamp)
PARTITIONED BY (
`year` int,
`period` string,
`distributorcode` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
location '/work/kap/menat_ksop/ims_actual/pcd'