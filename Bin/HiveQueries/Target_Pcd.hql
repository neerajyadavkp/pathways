-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract Target loaded in curent and previous cycles
-------------------------------------------------------------------------------
select 
year as year,
period as period,
kelloggbrand as kelloggbrand,
distributorcode as distributorcode,
volume as volume,
load_date as load_date
from amea.pcd_ext_orc_menat_ims_target
;