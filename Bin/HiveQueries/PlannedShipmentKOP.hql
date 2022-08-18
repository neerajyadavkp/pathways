-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract shipment orders from KOP 
-------------------------------------------------------------------------------
select
final.salesref as salesref,
final.createdon as createdon,
final.reqdeldate as reqdeldate,
final.soldto as soldto,
final.shipto as shipto,
final.customernamesoldto as customernamesoldto,
final.purchaseorderno as purchaseorderno,
final.shpt as shpt,
final.billto as billto,
final.material as material,
final.materialdesc as materialdesc,
final.category as category,
final.salesorg as salesorg,
final.netweight as netweight,
final.weightunit as weightunit,
final.cases as cases,
final.pallets as pallets,
final.market as market,
final.country as country,
final.transit_time as transit_time,
final.shipment_date as shipment_date,
final.shipment_period as shipment_period
from amea.uvw_menat_pathways_kop_planned_shipments as final
;