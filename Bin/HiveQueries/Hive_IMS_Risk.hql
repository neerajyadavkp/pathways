-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to compute for IMS Risk from 
--- amea.pcd_ext_orc_menat_pathway_drm_pipelineinput table
-------------------------------------------------------------------------------

SELECT dfu_code
,dfu_desc
,brand
,period
,year
,yearmonth
,distributorcode
,woc
,os
,ims
,risk
,reportkey 
FROM amea.uvw_ims_risk
where unix_timestamp(CONCAT(YEAR,'-',(CASE WHEN LENGTH(period)=1 then CONCAT('0',period) else period end),'-','01'), 'yyyy-MM-dd')  >= unix_timestamp(date_add(last_day(add_months(current_date, -6)),1), 'yyyy-MM-dd');
