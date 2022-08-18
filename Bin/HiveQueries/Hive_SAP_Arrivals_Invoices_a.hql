-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract shipment invoices from EAP and KOP SAP
-------------------------------------------------------------------------------
SELECT 
final.DocPeriod AS DocPeriod, 
final.ReqDelDate ReqDelDate, 
final.ShipmentDate AS ShipmentDate, 
final.ShipmentPeriod as ShipmentPeriod, 
final.DFU AS DFU,
final.SKU AS SKU, 
final.SoldTo AS SoldTo, 
final.Category AS Category, 
final.PlantCode AS PlantCode, 
final.PlantDesc AS PlantDesc, 
final.SalesDoc AS SalesDoc, 
final.Material AS Material, 
final.MaterialDesc AS MaterialDesc, 
final.Cases AS Cases,
final.Year as Year,
final.transittime as transittime,
final.ArrPeriod as ArrPeriod,
final.billto as billto
from
(
select * from amea.uvw_menat_pathways_kop_invoices 
union
select * from amea.uvw_menat_pathways_eap_invoices
) as final
;