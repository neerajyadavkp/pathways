-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract the shipment confirmation where orders has been received from the portal
-------------------------------------------------------------------------------
select billto  as DistributorCode ,
material as DFU,
plant as plant,
sum(sum_of_cases) as ARR,
ArrPeriod,
year
from amea.uvw_ext_orc_menat_pathway_web_arrival_confirmation
where shipment_year_period <= concat(year,lpad(ArrPeriod,2,0)) --consider the shipment till current cycle
group by billto,material,ArrPeriod,year,plant
;