-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create PCD table for 
---           web uploaded data .
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`pcd_ext_orc_menat_pathway_web_shipment_arrivals`(
`shipment_period` string,
`shipment_year_period` string,
`category` string,
`plantdesc` string,
`distributor_name` string,
`distributor_code` string,
`billto` string,
`plant` string,
`sales_document` string,
`material` string,
`description` string,
`sum_of_kilos` float,
`sum_of_gsv` float,
`sum_of_cases` float,
`purchase_order_number` int,
`billing_doc` int,
`invoice_date` string,
`plannedarrivalyear` string,
`transittime` string,
`plannedarrivalperiod` string,
`invoice_list_no` string,
`ets` string,
`eta` string,
`blno` string,
`containerno` string,
`arrived` string,
`financial_year` int,
`year` int,
`arrperiod` int,
`cycle` string,
`load_date` timestamp)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/work/kap/menat_ksop/web_shipment_arrivals/pcd'
