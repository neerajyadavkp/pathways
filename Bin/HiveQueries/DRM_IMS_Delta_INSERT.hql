INSERT OVERWRITE table amea.temp_DRM_IMS_Delta partition(REPORT_PERIOD,DISTRIBUTORCODE,BRAND,DFU,YEARPERIOD)
SELECT CASE
WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END REPORT_PERIOD
,ACTUAL.DISTRIBUTORCODE
,ACTUAL.BRAND
,ACTUAL.DFU
,ACTUAL.DESC
,ACTUAL.YEARPERIOD
,ACTUAL.IMSActual
,FORECAST.IMSForecast
,CAST(ACTUAL.IMSActual - FORECAST.IMSForecast AS DECIMAL(10,2)) IMSDelta
FROM
(
SELECT ACTL.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.new_dfu DFU
,DFUMap.new_desc DESC
,CASE
WHEN LENGTH(ACTL.period)=1 then CONCAT(ACTL.year,'0',ACTL.period) ELSE CONCAT(ACTL.year,ACTL.period) END YEARPERIOD
,ACTL.year YEAR
,ACTL.period PERIOD
,SUM(CAST((COALESCE(ACTL.sales,0) * DFUMap.conversion_factor_case_to_tons) AS DECIMAL(10,2))) IMSActual
FROM amea.src_ext_txt_menat_pathways_dfu_mapping DFUMap
JOIN amea.pcd_ext_orc_menat_pathway_ims_actual ACTL
on (DFUMap.new_dfu=ACTL.dfucode)
WHERE ACTL.year = ${hivevar:year} --reporting year parameter
and ACTL.period < ${hivevar:period} --reporting period parameter
GROUP BY ACTL.distributorcode
,DFUMap.BRAND
,DFUMap.new_dfu
,DFUMap.new_desc
,CASE
WHEN LENGTH(ACTL.period)=1 then CONCAT(ACTL.year,'0',ACTL.period) ELSE CONCAT(ACTL.year,ACTL.period) END
,ACTL.year
,ACTL.period
UNION
SELECT ACTL.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.new_dfu DFU
,DFUMap.new_desc DESC
,CASE
WHEN LENGTH(ACTL.period)=1 then CONCAT(ACTL.year,'0',ACTL.period) ELSE CONCAT(ACTL.year,ACTL.period) END YEARPERIOD
,ACTL.year YEAR
,ACTL.period PERIOD
,SUM(CAST((COALESCE(ACTL.sales,0) * DFUMap.conversion_factor_case_to_tons) AS DECIMAL(10,2))) IMSActual
FROM amea.src_ext_txt_menat_pathways_dfu_mapping DFUMap
JOIN amea.pcd_ext_orc_menat_pathway_ims_actual ACTL
on (DFUMap.new_dfu=ACTL.dfucode)
WHERE ACTL.year = ${hivevar:year}-1 --reporting year parameter
and ACTL.period < CASE WHEN ${hivevar:period}-4 < 1 THEN ${hivevar:period}+8 ELSE 0 END --reporting period parameter
GROUP BY ACTL.distributorcode
,DFUMap.BRAND
,DFUMap.new_dfu
,DFUMap.new_desc
,CASE
WHEN LENGTH(ACTL.period)=1 then CONCAT(ACTL.year,'0',ACTL.period) ELSE CONCAT(ACTL.year,ACTL.period) END
,ACTL.year
,ACTL.period
UNION
SELECT FCST.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.new_dfu DFU
,DFUMap.new_desc DESC
,CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END YEARPERIOD
,FCST.year YEAR
,FCST.period PERIOD
,SUM(CAST((COALESCE(FCST.sales,0) * DFUMap.conversion_factor_case_to_tons) AS DECIMAL(10,2))) IMSActual
FROM amea.src_ext_txt_menat_pathways_dfu_mapping DFUMap
JOIN amea.pcd_ext_orc_menat_pathway_ims_forecast FCST
on (DFUMap.new_dfu=FCST.dfucode)
WHERE FCST.period = ${hivevar:period} --reporting period parameter
AND FCST.year = ${hivevar:year} --reporting year parameter
GROUP BY FCST.distributorcode
,DFUMap.BRAND
,DFUMap.new_dfu
,DFUMap.new_desc
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
INNER JOIN
(
SELECT FCST.distributorcode DISTRIBUTORCODE
,DFUMap.BRAND
,DFUMap.new_dfu DFU
,DFUMap.new_desc DESC
,CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END YEARPERIOD
,FCST.year YEAR
,FCST.period PERIOD
,SUM(CAST((COALESCE(FCST.sales,0) * DFUMap.conversion_factor_case_to_tons) AS DECIMAL(10,2))) IMSForecast
FROM amea.pcd_ext_orc_menat_pathway_ims_forecast FCST
JOIN amea.src_ext_txt_menat_pathways_dfu_mapping DFUMap
on (DFUMap.new_dfu=FCST.dfucode)
WHERE FCST.period between (CASE WHEN ${hivevar:period}-5 < 1 THEN 0 ELSE ${hivevar:period}-5 END) and (CASE WHEN ${hivevar:period}-5 < 1 THEN 0 ELSE ${hivevar:period}-2 END) --reporting period parameter
AND FCST.year = ${hivevar:year} --reporting year parameter
GROUP BY FCST.distributorcode
,DFUMap.BRAND
,DFUMap.new_dfu
,DFUMap.new_desc
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
,DFUMap.new_dfu DFU
,DFUMap.new_desc DESC
,CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END YEARPERIOD
,FCST.year YEAR
,FCST.period PERIOD
,SUM(CAST((COALESCE(FCST.sales,0) * DFUMap.conversion_factor_case_to_tons) AS DECIMAL(10,2))) IMSForecast
FROM amea.pcd_ext_orc_menat_pathway_ims_forecast FCST
JOIN amea.src_ext_txt_menat_pathways_dfu_mapping DFUMap
on (DFUMap.new_dfu=FCST.dfucode)
WHERE FCST.period between (CASE WHEN ${hivevar:period}-5 < 1 THEN ${hivevar:period}+6 ELSE 0 END) and (CASE WHEN ${hivevar:period}-5 < 1 THEN ${hivevar:period}+9 ELSE 0 END) --reporting period parameter
AND FCST.year = ${hivevar:year}-1 --reporting year parameter
GROUP BY FCST.distributorcode
,DFUMap.BRAND
,DFUMap.new_dfu
,DFUMap.new_desc
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
,DFUMap.new_dfu DFU
,DFUMap.new_desc DESC
,CASE
WHEN LENGTH(FCST.yearperiod)=7 then CONCAT(SUBSTR(FCST.yearperiod,-4),SUBSTR(FCST.yearperiod,2,2)) ELSE CONCAT(SUBSTR(FCST.yearperiod,-4),CONCAT('0',SUBSTR(FCST.yearperiod,2,1))) END YEARPERIOD
,FCST.year YEAR
,FCST.period PERIOD
,SUM(CAST((COALESCE(FCST.sales,0) * DFUMap.conversion_factor_case_to_tons) AS DECIMAL(10,2))) IMSForecast
FROM amea.src_ext_txt_menat_pathways_dfu_mapping DFUMap
JOIN amea.pcd_ext_orc_menat_pathway_ims_forecast FCST
on (DFUMap.new_dfu=FCST.dfucode)
WHERE FCST.period = (${hivevar:period}-1) --reporting period parameter -1
AND FCST.year = ${hivevar:year} --reporting year parameter
GROUP BY FCST.distributorcode
,DFUMap.BRAND
,DFUMap.new_dfu
,DFUMap.new_desc
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
ON FORECAST.distributorcode = ACTUAL.distributorcode
and FORECAST.BRAND = ACTUAL.BRAND
and FORECAST.DFU = ACTUAL.DFU
and FORECAST.DESC = ACTUAL.DESC
and FORECAST.YEARPERIOD = ACTUAL.YEARPERIOD;