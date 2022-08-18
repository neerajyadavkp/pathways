-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create pcd table for 
---           IMS Material data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`pcd_ext_orc_menat_ims_material`(
`business_split` string,
`category` string,
`material_group` string,
`sub_category` string,
`market_segment` string,
`brand` string,
`sub_brand` string,
`net_weight_kgs` float,
`pack_size_code` string,
`pack_format` string,
`sku_description` string,
`sku` string,
`conversion_factor` float,
`ims_brand` string,
`load_date` timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/work/kap/menat_dsr/ims_material/pcd'