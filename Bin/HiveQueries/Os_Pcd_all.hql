-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract Opening stock loaded in cuurent and previous cycles
-------------------------------------------------------------------------------
select 
dfucode as dfucode,
volume as volume,
load_date as load_date,
year as year,
period as period,
distributorcode as distributorcode
from amea.pcd_ext_orc_menat_pathway_os
;
