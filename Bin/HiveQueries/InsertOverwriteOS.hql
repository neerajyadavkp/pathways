-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to create Insert the OS upload into PCD table
-------------------------------------------------------------------------------
insert overwrite table amea.pcd_ext_orc_menat_pathway_os partition(year,period,distributorcode)
select  dfucode          ,  
	dfudescription,
	kelloggbrand,                        
		volume           ,                   
		from_unixtime(unix_timestamp(load_date,'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as load_date,               
		year             ,                    
		period           ,                  
		distributorcode  
from amea.src_ext_txt_menat_pathway_os;
