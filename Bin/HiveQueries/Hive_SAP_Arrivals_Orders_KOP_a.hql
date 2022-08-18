-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to extract shipment orders from KOP
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
from amea.uvw_menat_pathways_kop_orders as final
;