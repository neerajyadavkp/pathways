-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create archive table for 
---           IMS Customer data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`archive_ext_txt_menat_ims_customer`(
`bu` string,
`bu_region` string,
`country` string,
`country_region` string,
`channel` string,
`subchannel` string,
`customer_name` string,
`sold_to_customer_code` int,
`load_date` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_dsr/ims_customer/archive'
TBLPROPERTIES (
'skip.header.line.count'='1'
)