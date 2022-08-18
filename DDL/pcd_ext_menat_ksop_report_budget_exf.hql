CREATE EXTERNAL TABLE
amea.pcd_ext_menat_ksop_report_budget_exf
(Market String
,Distributor string
,Sold_To_Party string
,NEW_DFU string
,NEW_DESC string
,BRAND string
,Month int
,EXF float
,Load_Date timestamp
)
PARTITIONED BY (Year int
,Version string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/work/kap/menat_ksop/ksop_report/pcd_ext_menat_ksop_report_budget_exf'
TBLPROPERTIES ("skip.header.line.count"="1");