-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract Customer mapping loaded in curent and previous cycles
-------------------------------------------------------------------------------
select 
bu as bu,
bu_region as bu_region,
country as country,
country_region as country_region,
channel as channel,
subchannel as subchannel,
customer_name as customer_name,
sold_to_customer_code,
load_date as load_date
from amea.pcd_ext_orc_menat_ims_customer
;