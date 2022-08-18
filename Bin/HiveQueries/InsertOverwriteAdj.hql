--******* Adj ******
use amea;
insert overwrite table amea.pcd_ext_orc_menat_pathway_adj_stock partition(year,period,distributorcode)
select  dfucode ,
volume ,
from_unixtime(unix_timestamp(current_timestamp(),'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as load_date,
year ,
period ,
distributorcode
from amea.src_ext_txt_menat_pathway_adj_stock;