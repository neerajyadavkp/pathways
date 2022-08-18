-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract Actual IMS Uploaded and loaded in PCD tables for integrated pipeline processing
-------------------------------------------------------------------------------
select  
dfucode as dfucode, 
dfudescription as dfudescription,
kelloggbrand as kelloggbrand ,
packsize as packsize,
caseweight as caseweight,
promo_non_promo as promo_non_promo,
channel as channel ,
subchannel as subchannel ,
sales as sales ,
distributorgsv as distributorgsv, 
load_date as load_date ,
year as year ,
period as period,
distributorcode as distributorcode
from amea.pcd_ext_orc_menat_pathway_ims_actual as act
inner join
( ---- ***Getting the latest Year, if period control is set for today's date it will pick the Year from the file ***----
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
on 1=1
where  cast(kin2.cycle_year as int)-1 <= (cast(act.year as int))
;