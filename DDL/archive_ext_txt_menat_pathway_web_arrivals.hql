-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create archive table for 
---           web uploaded data .
-------------------------------------------------------------------------------
CREATE TABLE amea.`archive_ext_txt_menat_pathway_web_arrivals`(
`shipment_period` string,
`category` string,
`plantdesc` string,
`distributor_name` string,
`distributor_code` string,
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
`sales_order` int,
`ets` string,
`eta` string,
`blno` string,
`containerno` string,
`arrived` string,
`load_date` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_ksop/web_shipment_arrivals/archive'
TBLPROPERTIES (
'skip.header.line.count'='1'
)