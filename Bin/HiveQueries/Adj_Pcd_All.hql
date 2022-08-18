-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract Previous Month Adjustment value which is calculated in main python code and stored to be used in new cycle
-------------------------------------------------------------------------------
select 
dfucode as dfucode ,
volume as volume ,
load_date as load_date,
year as year ,
period as period ,
distributorcode as distributorcode
from  amea.pcd_ext_orc_menat_pathway_adj_stock
;