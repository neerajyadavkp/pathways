-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract the shipments where confirmation is not provided , treated as leftover and will added to cuurent month SAP Arrivals in Python code
-------------------------------------------------------------------------------
select 
billto as DistributorCode,
material as DFU,
plant as plant,
sum(sum_of_cases) as ARR
from amea.uvw_ext_orc_menat_pathway_web_arrival_no_confirmation
where concat(int(plannedarrivalyear),lpad(int(plannedarrivalperiod),2,0)) < concat(financial_year,lpad(substr(cycle,2),2,0))
group by billto ,material,plant
;