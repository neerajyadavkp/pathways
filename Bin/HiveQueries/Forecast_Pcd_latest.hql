-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract Forecast IMS Uploaded
-------------------------------------------------------------------------------
select  
fc.dfucode as dfucode,                                      
fc.dfudescription as dfudescription ,                              
fc.kelloggbrand as kelloggbrand,                             
fc.packsize as packsize ,                              
fc.caseweight as caseweight,                            
fc.promo_nonpromo as promo_nonpromo,                               
fc.channel as channel,                               
fc.subchannel as subchannel,                               
fc.yearperiod as yearperiod,                             
fc.sales as sales,                               
fc.load_date as load_date,                 
fc.year as year,                                  
fc.period as period,
fc.distributorcode as distributorcode,                               
fc.version as version 
from amea.pcd_ext_orc_menat_pathway_ims_forecast fc
inner join ( ---- ***Getting the latest cycle, if period control is set for today's date it will pick the cycle from the file ***----
select case when ctr.load_date=current_date() then (ctr.cycle)
else 
src.month_no 
end as cycle,
case when ctr.load_date=current_date() then (ctr.cycle_year)
else 
src.financial_year 
end as cycle_year
from 
(select month_no,daate,financial_year
from amea.pcd_ext_orc_kin_f_date 
where daate=current_date()) src
left join amea.src_ext_txt_menat_ksop_cycle_controllers ctr
on src.daate = ctr.load_date
) kin2
on cast(kin2.cycle as int) = cast(fc.period as int) and cast(kin2.cycle_year as int) = cast(fc.year as int)
inner join ( ---- *** getting the distributor name for which latest forecast file is uploaded ***----
select   distinct
ims_f.distributorcode,
load_date as load_date,                            
version as version 
from amea.pcd_ext_orc_menat_pathway_ims_forecast as ims_f
inner join 
( 
select distributorcode,max(load_date) as max_load_dt from amea.pcd_ext_orc_menat_pathway_ims_forecast
group by distributorcode
) as max_dt
on ims_f.distributorcode=max_dt.distributorcode and max_load_dt=load_date
inner join
( -- *** Join to filter only the latest data based on overall max load date **--
select max(load_date) as max_load_dt_total from amea.pcd_ext_orc_menat_pathway_ims_forecast
) as max_dt_total 
on ims_f.distributorcode=max_dt.distributorcode and max_load_dt=load_date and max_dt_total.max_load_dt_total=max_dt.max_load_dt
) date_version
on  fc.version = date_version.version
and date_version.distributorcode= fc.distributorcode and date_version.load_date=fc.load_date
;