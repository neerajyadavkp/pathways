-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to Insert IMS Target into PCD table
-------------------------------------------------------------------------------
insert overwrite table amea.pcd_ext_orc_menat_ims_target partition(year,period,distributorcode)
select  kelloggbrand,
		volume,
		from_unixtime(unix_timestamp(load_date,'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as load_date,
		year,
		period,
		distributorcode
from amea.src_ext_txt_menat_ims_target;