-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract invoices from KOP 
-------------------------------------------------------------------------------
select  
final.`Shipment Period` as `shipment period`,
final.shipment_year_period as shipment_year_period,
final.Category as category,
final.plantDesc as plantDesc,
final.distributor_name as distributor_name,
final.distributor_code as distributor_code,
final.billto as billto,
int(final.plant) as plant,
final.`sales document` as `sales document`,
final.material as material,
final.description as description,
final.`sum of kilos` as `sum of kilos`,
final.`sum of gsv` as `sum of gsv`,
final.`sum of cases` as `sum of cases`,
final.`purchase order number` as `purchase order number`,
final.`billing doc` as `billing doc`,
final.`Invoice Date` as `invoice date`,
final.plannedarrivalyear as plannedarrivalyear,
final.transittime as transittime,
final.plannedArrivalPeriod as plannedArrivalPeriod 
from
amea.uvw_menat_pathways_kop_actual_shipments as final
;