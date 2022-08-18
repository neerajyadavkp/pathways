CREATE EXTERNAL TABLE amea.pcd_ext_menat_ksop_rno(
Country string,
Category string,
RnO_Component string,
RnO_Class string,
RnO_value int,
RYear int,
RMonth string,
Period string,
Load_date timestamp)
PARTITIONED BY (
year int,
report_period int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/work/kap/menat_ksop/gm_report/pcd_ext_menat_ksop_rno'




Country string,
Category string,
RnO_Component string,
RnO_Class string,
RnO_value int,
RYear int,
RMonth string,
Period string,
Load_date timestamp