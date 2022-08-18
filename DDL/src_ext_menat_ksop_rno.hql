CREATE EXTERNAL TABLE
amea.src_ext_menat_ksop_rno
(Year int,
Report_Period string,
Country string,
RnO_Component string,
Category string,
RnO_Class string,
RYear int,
Period string,
RnO_value int,
RMonth string,
Load_date timestamp

)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_ksop/gm_report/src_ext_menat_ksop_rno'




