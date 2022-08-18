-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract Material mapping loaded in curent and previous cycles
-------------------------------------------------------------------------------
select 
business_split as business_split,
category as category,
material_group as material_group,
sub_category as sub_category,
market_segment as market_segment,
brand as brand,
sub_brand as sub_brand,
net_weight_kgs as net_weight_kgs,
pack_size_code as pack_size_code,
pack_format as pack_format,
sku_description as sku_description,
sku as sku,
conversion_factor as conversion_factor,
ims_brand as ims_brand,
load_date as load_date
from amea.pcd_ext_orc_menat_ims_material
;