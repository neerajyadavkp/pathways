-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract Daily Actuals loaded in curent and previous cycles
-------------------------------------------------------------------------------
select 
kelloggskucode as kelloggskucode, 
kelloggskudescription as kelloggskudescription,
kelloggbrand as kelloggbrand,
packsizegrams as packsizegrams,
caseweightgrams as caseweightgrams,
promo_non_promo as promo_non_promo,
channel as channel,
subchannel as subchannel,
salesincases as salesincases,
distributorgsv as distributorgsv, 
load_date as load_date,
year as year,
period as period,
distributorcode as distributorcode,
short_load_date as short_load_date,
delta_salesincases as delta_salesincases
from amea.pcd_ext_orc_menat_ims_actual_daily
;