-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create table for 
---           DRM IMS Delta data.
-------------------------------------------------------------------------------
CREATE EXTERNAL TABLE amea.`temp_DRM_IMS_Delta`(
`BRAND` string,
`DFU` string,
`DESC` string,
`IMSActual` float,
`IMSForecast` float,
`IMSDelta` float)
PARTITIONED BY (
`REPORT_PERIOD` int,
`DISTRIBUTORCODE` string,
`YEARPERIOD` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC
location '/work/kap/menat_ksop/drm_ims_delta';
