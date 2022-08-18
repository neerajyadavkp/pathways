-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to Insert IMS Material into PCD table
-------------------------------------------------------------------------------
insert overwrite table amea.pcd_ext_orc_menat_ims_material
select  business_split,
		category,
		material_group,
		sub_category,
		market_segment,
		brand,
		sub_brand,
		net_weight_kgs,
		pack_size_code,
		pack_format,
		sku_description,
		sku,
		conversion_factor,
		ims_brand,
		from_unixtime(unix_timestamp(load_date,'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as load_date
from amea.src_ext_txt_menat_ims_material;