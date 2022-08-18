-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 3
--- Region  : AMEA
--- Purpose : STATIC reference table for TABLEAU Reports(KSOP MODEL/KSOP Report)
---           
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE `amea.src_ext_txt_menat_ksop_tableau_extract`(
`section_name` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION
'/work/kap/menat_ksop/ksop_model/src_ext_txt_menat_ksop_tableau_extract';

INSERT INTO amea.src_ext_txt_menat_ksop_tableau_extract  VALUES ('Succeeding Year'),('Succeeding Year Budget') ,('Previous Cycle'),('Budget'),('Previous Year');