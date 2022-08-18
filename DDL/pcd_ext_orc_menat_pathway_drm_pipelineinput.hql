CREATE EXTERNAL TABLE amea.`pcd_ext_orc_menat_pathway_drm_pipelineinput`(
`quarter` string,
`month` string,
`yearmonth` string,
`period` string,
`distributorcode` string,
`country` string,
`sku` string,
`skudescription` string,
`brand` string,
`category` string,
`country_group` string,
`conversion_factor` double,
`os` double,
`arr` double,
`ims` double,
`woc` double,
`adj` double,
`flag` int,
`stock_days` double,
`load_date` timestamp)
PARTITIONED BY (
`year` int,
`cycle` string,
`distributorname` string,
`version` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION 
'/work/kap/menat_ksop/drm_pipelineinput/pcd'