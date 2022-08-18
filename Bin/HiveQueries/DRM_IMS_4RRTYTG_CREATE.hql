--YTD Vs. YTG Report 4RR
DROP TABLE IF EXISTS amea.temp_DRM_IMS_4RRTYTG;
CREATE TABLE amea.temp_DRM_IMS_4RRTYTG AS
SELECT DELTA.REPORT_PERIOD
,DELTA.DISTRIBUTORCODE
,DELTA.BRAND
,DELTA.DFU
,DELTA.DESC
,DELTA.AVGActual
,DELTA2.AVGForecast
,DELTA.AVGActual - DELTA2.AVGForecast AVGDelta
FROM
(
SELECT D.REPORT_PERIOD
,D.DISTRIBUTORCODE
,D.BRAND
,D.DFU
,D.DESC
,SUM(D.IMSActual)/4 AVGActual
FROM amea.temp_DRM_IMS_Delta D
WHERE D.YEARPERIOD BETWEEN CAST(CASE WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END AS INT)-4 AND CAST(CASE WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END AS INT)-1 --rolling value for 4 months
AND REPORT_PERIOD = CASE WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END
GROUP BY D.REPORT_PERIOD
,D.DISTRIBUTORCODE
,D.BRAND
,D.DFU
,D.DESC
) DELTA
INNER JOIN
(SELECT D2.REPORT_PERIOD
,D2.DISTRIBUTORCODE
,D2.BRAND
,D2.DFU
,D2.DESC
,SUM(D2.IMSActual)/4 AVGForecast
FROM amea.temp_DRM_IMS_Delta D2
WHERE D2.YEARPERIOD BETWEEN CAST(CASE WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END AS INT) AND CAST(CASE WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END AS INT)+3  --rolling value for 4months
AND REPORT_PERIOD = CASE WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END
GROUP BY D2.REPORT_PERIOD
,D2.DISTRIBUTORCODE
,D2.BRAND
,D2.DFU
,D2.DESC
) DELTA2
ON DELTA2.REPORT_PERIOD = DELTA.REPORT_PERIOD
and DELTA2.DISTRIBUTORCODE = DELTA.DISTRIBUTORCODE
and DELTA2.BRAND = DELTA.BRAND
and DELTA2.DFU = DELTA.DFU;