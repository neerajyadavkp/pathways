-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 2
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to Insert IMS Actual Daily into PCD table
-------------------------------------------------------------------------------
insert overwrite table amea.pcd_ext_orc_menat_ims_actual_daily partition(year,period,distributorcode,short_load_date)
select  kelloggskucode,       
		kelloggskudescription, 
		kelloggbrand,
		packsizegrams,  
		caseweightgrams,    
		promo_non_promo,
		channel,        
		subchannel,     
		salesincases,          
		distributorgsv, 
		load_date,
		delta_salesincases,
		year,           
		period,         
		distributorcode,
		short_load_date
from amea.src_ext_txt_menat_ims_actual_daily;