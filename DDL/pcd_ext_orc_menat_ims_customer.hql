-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           IMS Customer data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`pcd_ext_orc_menat_ims_customer`(
`bu` string,
`bu_region` string,
`country` string,
`country_region` string,
`channel` string,
`subchannel` string,
`customer_name` string,
`sold_to_customer_code` int,
`load_date` timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/work/kap/menat_dsr/ims_customer/pcd'