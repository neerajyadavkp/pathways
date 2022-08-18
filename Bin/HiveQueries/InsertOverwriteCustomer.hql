-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to Insert IMS Customer into PCD table
-------------------------------------------------------------------------------
insert overwrite table amea.pcd_ext_orc_menat_ims_customer
select  bu,
		bu_region,
		country,
		country_region,
		channel,
		subchannel,
		customer_name,
		sold_to_customer_code,
		from_unixtime(unix_timestamp(load_date,'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as load_date
from amea.src_ext_txt_menat_ims_customer;