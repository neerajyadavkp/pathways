--YTD Vs. YTG Report
INSERT INTO table amea.temp_DRM_IMS_YTDYTG
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
,SUM(D.IMSActual)/(CAST(${hivevar:period} AS INT)-1) AVGActual
FROM amea.temp_DRM_IMS_Delta D
WHERE D.YEARPERIOD BETWEEN CAST(CONCAT(${hivevar:year},'01') AS INT) AND CAST(CASE WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END AS INT)-1 --ytd
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
,SUM(D2.IMSActual)/(13-CAST(${hivevar:period} AS INT)) AVGForecast
FROM amea.temp_DRM_IMS_Delta D2
WHERE D2.YEARPERIOD BETWEEN CAST(CASE WHEN LENGTH(${hivevar:period})=1 then CONCAT(${hivevar:year},'0',${hivevar:period}) ELSE CONCAT(${hivevar:year},${hivevar:period}) END AS INT) AND CAST(CONCAT(${hivevar:year},'12') AS INT) --ytd
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