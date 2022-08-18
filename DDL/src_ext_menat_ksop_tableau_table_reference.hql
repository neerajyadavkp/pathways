-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 3
--- Region  : AMEA
--- Purpose : STATIC reference table for TABLEAU Reports(KSOP MODEL/KSOP Report)
---           
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE `amea.src_ext_txt_menat_ksop_tableau_table_reference`(
`table` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION
'/work/kap/menat_ksop/ksop_model/src_ext_txt_menat_ksop_tableau_table_reference';


INSERT INTO amea.src_ext_txt_menat_ksop_tableau_table_reference VALUES ('selected 1'),('selected 2'),('compared 1'),('compared 2'),('difference 1'),('difference 2');