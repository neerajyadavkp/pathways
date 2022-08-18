INSERT OVERWRITE table amea.temp_DRM_IMS_Delta partition(REPORT_PERIOD,DISTRIBUTORCODE,YEARPERIOD)
SELECT CASE WHEN (ACTUAL.BRAND IS NULL) THEN FORECAST.BRAND ELSE ACTUAL.BRAND END BRAND
,CASE WHEN (ACTUAL.DFU IS NULL) THEN FORECAST.DFU ELSE ACTUAL.DFU END DFU
,CASE WHEN (ACTUAL.DESC IS NULL) THEN FORECAST.DESC ELSE ACTUAL.DESC END DESC
,COALESCE(ACTUAL.IMSActual,0) IMSActual
,COALESCE(FORECAST.IMSForecast,0) IMSForecast
,COALESCE(ACTUAL.IMSActual,0) - COALESCE(FORECAST.IMSForecast,0) IMSDelta
,CASE
WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END REPORT_PERIOD
,CASE WHEN (ACTUAL.DISTRIBUTORCODE IS NULL) THEN FORECAST.DISTRIBUTORCODE ELSE ACTUAL.DISTRIBUTORCODE END DISTRIBUTORCODE
,CASE WHEN (ACTUAL.YEARPERIOD IS NULL) THEN FORECAST.YEARPERIOD ELSE ACTUAL.YEARPERIOD END YEARPERIOD
FROM
(
SELECT ACTL.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.DFU DFU
,DFUMap.DESC DESC
,CASE
WHEN LENGTH(ACTL.period)=1 then CONCAT(ACTL.year,'0',ACTL.period) ELSE CONCAT(ACTL.year,ACTL.period) END YEARPERIOD
,ACTL.year YEAR
,ACTL.period PERIOD
,SUM(COALESCE(ACTL.sales,0) * DFUMap.conversion_factor_case_to_tons) IMSActual
FROM amea.pcd_ext_orc_menat_pathway_ims_actual ACTL
JOIN
(  
SELECT DISTINCT DF.BRAND, DF.DFU, DF.DESC, DF.CONVERSION_FACTOR_CASE_TO_TONS
FROM
(SELECT DISTINCT DF1.BRAND, DF1.new_dfu DFU, DF1.new_desc DESC, DF1.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF1
  UNION
  SELECT DISTINCT DF2.BRAND, DF2.old_dfu DFU, DF2.new_desc DESC, DF2.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF2
 ) DF
)DFUMap
   on (DFUMap.DFU=ACTL.dfucode) 
WHERE ACTL.year = ${hivevar:year} --reporting year parameter
and ACTL.period < ${hivevar:period} --reporting period parameter
GROUP BY ACTL.distributorcode
,DFUMap.BRAND
,DFUMap.DFU
,DFUMap.DESC
,CASE
WHEN LENGTH(ACTL.period)=1 then CONCAT(ACTL.year,'0',ACTL.period) ELSE CONCAT(ACTL.year,ACTL.period) END
,ACTL.year
,ACTL.period
UNION
SELECT ACTL.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.DFU DFU
,DFUMap.DESC DESC
,CASE
WHEN LENGTH(ACTL.period)=1 then CONCAT(ACTL.year,'0',ACTL.period) ELSE CONCAT(ACTL.year,ACTL.period) END YEARPERIOD
,ACTL.year YEAR
,ACTL.period PERIOD
,SUM(COALESCE(ACTL.sales,0) * DFUMap.conversion_factor_case_to_tons) IMSActual
FROM amea.pcd_ext_orc_menat_pathway_ims_actual ACTL
JOIN
(  
SELECT DISTINCT DF.BRAND, DF.DFU, DF.DESC, DF.CONVERSION_FACTOR_CASE_TO_TONS
FROM
(SELECT DISTINCT DF1.BRAND, DF1.new_dfu DFU, DF1.new_desc DESC, DF1.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF1
  UNION
  SELECT DISTINCT DF2.BRAND, DF2.old_dfu DFU, DF2.new_desc DESC, DF2.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF2
 ) DF
)DFUMap
   on (DFUMap.DFU=ACTL.dfucode)  
WHERE ACTL.year = ${hivevar:year}-1 --reporting year parameter
and ACTL.period < CASE WHEN ${hivevar:period}-4 < 1 THEN ${hivevar:period}+8 ELSE 0 END --reporting period parameter
GROUP BY ACTL.distributorcode
,DFUMap.BRAND
,DFUMap.DFU
,DFUMap.DESC
,CASE
WHEN LENGTH(ACTL.period)=1 then CONCAT(ACTL.year,'0',ACTL.period) ELSE CONCAT(ACTL.year,ACTL.period) END
,ACTL.year
,ACTL.period
UNION
SELECT FCST.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.DFU DFU
,DFUMap.DESC DESC
,CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END YEARPERIOD
,FCST.year YEAR
,FCST.period PERIOD
,SUM(COALESCE(FCST.sales,0) * DFUMap.conversion_factor_case_to_tons) IMSActual
FROM amea.pcd_ext_orc_menat_pathway_ims_forecast FCST
JOIN
(  
SELECT DISTINCT DF.BRAND, DF.DFU, DF.DESC, DF.CONVERSION_FACTOR_CASE_TO_TONS
FROM
(SELECT DISTINCT DF1.BRAND, DF1.new_dfu DFU, DF1.new_desc DESC, DF1.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF1
  UNION
  SELECT DISTINCT DF2.BRAND, DF2.old_dfu DFU, DF2.new_desc DESC, DF2.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF2
 ) DF
)DFUMap
   on (DFUMap.DFU=FCST.dfucode)  
WHERE FCST.period = ${hivevar:period} --reporting period parameter
AND FCST.year = ${hivevar:year} --reporting year parameter
GROUP BY FCST.distributorcode
,DFUMap.BRAND
,DFUMap.DFU
,DFUMap.DESC
,FCST.yearperiod
,FCST.year
,FCST.period
, CASE
WHEN LENGTH(FCST.yearperiod)=7 then SUBSTR(FCST.yearperiod,2,2) ELSE SUBSTR(FCST.yearperiod,2,1) END
, CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END
HAVING CASE
WHEN LENGTH(FCST.yearperiod)=7 then CAST(CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) AS INT) ELSE CAST(CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,1)) AS INT) END >= CAST(CONCAT(FCST.year, FCST.period) AS INT)
) AS ACTUAL
FULL OUTER JOIN
(
SELECT FCST.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.DFU DFU
,DFUMap.DESC DESC
,CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END YEARPERIOD
,FCST.year YEAR
,FCST.period PERIOD
,SUM(COALESCE(FCST.sales,0) * DFUMap.conversion_factor_case_to_tons) IMSForecast
FROM amea.pcd_ext_orc_menat_pathway_ims_forecast FCST
JOIN
(  
SELECT DISTINCT DF.BRAND, DF.DFU, DF.DESC, DF.CONVERSION_FACTOR_CASE_TO_TONS
FROM
(SELECT DISTINCT DF1.BRAND, DF1.new_dfu DFU, DF1.new_desc DESC, DF1.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF1
  UNION
  SELECT DISTINCT DF2.BRAND, DF2.old_dfu DFU, DF2.new_desc DESC, DF2.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF2
 ) DF
)DFUMap
   on (DFUMap.DFU=FCST.dfucode) 
WHERE FCST.period between (CASE WHEN ${hivevar:period}-5 < 1 THEN 0 ELSE ${hivevar:period}-5 END) and (CASE WHEN ${hivevar:period}-5 < 1 THEN 0 ELSE ${hivevar:period}-2 END) --reporting period parameter
AND FCST.year = ${hivevar:year} --reporting year parameter
GROUP BY FCST.distributorcode
,DFUMap.BRAND
,DFUMap.DFU
,DFUMap.DESC
,FCST.yearperiod
,FCST.year
,FCST.period
, CASE
WHEN LENGTH(FCST.yearperiod)=7 then SUBSTR(FCST.yearperiod,2,2) ELSE SUBSTR(FCST.yearperiod,2,1) END
, CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END
HAVING
CASE
WHEN LENGTH(FCST.yearperiod)=6 then SUBSTR(FCST.yearperiod,2,1) ELSE SUBSTR(FCST.yearperiod,2,2) END = fcst.period
AND SUBSTR(FCST.yearperiod,-4) = fcst.year
UNION
SELECT FCST.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.DFU DFU
,DFUMap.DESC DESC
,CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END YEARPERIOD
,FCST.year YEAR
,FCST.period PERIOD
,SUM(COALESCE(FCST.sales,0) * DFUMap.conversion_factor_case_to_tons) IMSForecast
FROM amea.pcd_ext_orc_menat_pathway_ims_forecast FCST
JOIN
(  
SELECT DISTINCT DF.BRAND, DF.DFU, DF.DESC, DF.CONVERSION_FACTOR_CASE_TO_TONS
FROM
(SELECT DISTINCT DF1.BRAND, DF1.new_dfu DFU, DF1.new_desc DESC, DF1.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF1
  UNION
  SELECT DISTINCT DF2.BRAND, DF2.old_dfu DFU, DF2.new_desc DESC, DF2.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF2
 ) DF
)DFUMap
   on (DFUMap.DFU=FCST.dfucode) 
WHERE FCST.period between (CASE WHEN ${hivevar:period}-5 < 1 THEN ${hivevar:period}+6 ELSE 0 END) and (CASE WHEN ${hivevar:period}-5 < 1 THEN ${hivevar:period}+9 ELSE 0 END) --reporting period parameter
AND FCST.year = ${hivevar:year}-1 --reporting year parameter
GROUP BY FCST.distributorcode
,DFUMap.BRAND
,DFUMap.DFU
,DFUMap.DESC
,FCST.yearperiod
,FCST.year
,FCST.period
, CASE
WHEN LENGTH(FCST.yearperiod)=7 then SUBSTR(FCST.yearperiod,2,2) ELSE SUBSTR(FCST.yearperiod,2,1) END
, CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END
HAVING
CASE
WHEN LENGTH(FCST.yearperiod)=6 then SUBSTR(FCST.yearperiod,2,1) ELSE SUBSTR(FCST.yearperiod,2,2) END = fcst.period
AND SUBSTR(FCST.yearperiod,-4) = fcst.year
UNION
SELECT FCST.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.DFU DFU
,DFUMap.DESC DESC
,CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END YEARPERIOD
,FCST.year YEAR
,FCST.period PERIOD
,SUM(COALESCE(FCST.sales,0) * DFUMap.conversion_factor_case_to_tons) IMSForecast
FROM amea.pcd_ext_orc_menat_pathway_ims_forecast FCST
JOIN
(  
SELECT DISTINCT DF.BRAND, DF.DFU, DF.DESC, DF.CONVERSION_FACTOR_CASE_TO_TONS
FROM
(SELECT DISTINCT DF1.BRAND, DF1.new_dfu DFU, DF1.new_desc DESC, DF1.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF1
  UNION
  SELECT DISTINCT DF2.BRAND, DF2.old_dfu DFU, DF2.new_desc DESC, DF2.conversion_factor_case_to_tons
  from  amea.src_ext_txt_menat_pathways_dfu_mapping DF2
 ) DF
)DFUMap
   on (DFUMap.DFU=FCST.dfucode) 
WHERE FCST.period = (${hivevar:period}-1) --reporting period parameter -1
AND FCST.year = ${hivevar:year} --reporting year parameter
GROUP BY FCST.distributorcode
,DFUMap.BRAND
,DFUMap.DFU
,DFUMap.DESC
,FCST.yearperiod
,FCST.year
,FCST.period
, CASE
WHEN LENGTH(FCST.yearperiod)=7 then SUBSTR(FCST.yearperiod,2,2) ELSE SUBSTR(FCST.yearperiod,2,1) END
, CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END
HAVING
CASE
WHEN LENGTH(FCST.yearperiod)=7 then CAST(CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) AS INT) ELSE CAST(CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,1)) AS INT) END >= CAST(CONCAT(FCST.year, FCST.period) AS INT)
) AS FORECAST
ON ACTUAL.distributorcode = FORECAST.distributorcode
and ACTUAL.BRAND = FORECAST.BRAND
and ACTUAL.DFU = FORECAST.DFU
and ACTUAL.DESC = FORECAST.DESC
and ACTUAL.YEARPERIOD = FORECAST.YEARPERIOD;