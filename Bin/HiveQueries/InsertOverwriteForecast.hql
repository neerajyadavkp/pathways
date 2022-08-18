-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to insert forecast data in PCD table
-------------------------------------------------------------------------------
insert overwrite table amea.pcd_ext_orc_menat_pathway_ims_forecast partition(year,period,distributorcode,version)
select  dfucode,                                          
		dfudescription,                              
		kelloggbrand,                             
		packsize   ,                              
		caseweight ,                            
		promo_nonpromo,                               
		channel,                               
		subchannel,                               
		yearperiod,                             
		sales,                               
		from_unixtime(unix_timestamp(load_date,'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as load_date,                 
		year,                                  
		period ,
		distributorcode,                               
		version
from amea.src_ext_txt_menat_pathway_ims_forecast;
