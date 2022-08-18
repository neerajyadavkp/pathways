-------------------------------------------------------------------------------
--- Project : MENAT Pathways - Phase - 1
--- Region  : AMEA
--- Purpose : The purpose of this SQL is to Insert the Shipment Confirmation into PCD Tables, with Arrived Period
-------------------------------------------------------------------------------
insert into table  amea.pcd_ext_orc_menat_pathway_web_shipment_arrivals
select
final.shipment_period,
final.shipment_year_period,
final.category,
final.plantdesc,
final.distributor_name,
final.distributor_code,
final.billto,
final.plant,
final.sales_document,
final.material,
final.description,
final.sum_of_kilos,
final.sum_of_gsv,
final.sum_of_cases,
final.purchase_order_number,
final.billing_doc,
final.invoice_date,
final.plannedarrivalyear,
final.transittime,
final.plannedarrivalperiod,
final.invoice_list_no, 
final.ets,
final.eta,
final.blno,
final.containerno,
final.arrived,
dt.financial_year,
case    
	when cast(ArrivedPeriod as int)=1 then cast((dt.financial_year-1) as int)
	else dt.financial_year
end as year,
--CONCAT('P+', sap.arrival_values) as transittime,
case   -- *** Confirmation will be for Prior period , thats why subtracting 1 from Arrived Period ***---
	when cast(ArrivedPeriod as int)=1 then 12
	else cast((ArrivedPeriod-1) as int)
end as ArrPeriod,
concat('P',ArrivedPeriod) as Cycle,
from_unixtime(unix_timestamp(current_timestamp(),'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss') as load_date
from amea.src_ext_txt_menat_pathway_web_arrivals final
--inner join
--amea.pcd_ext_orc_kin_f_date as dt on dt.DAATE = final.invoice_date
inner join
( 
select distinct c.distributor,c.stp as sold_to_codes,p.plant,p.arrival_values,c.distributor as sold_to_name
from amea.src_ext_txt_menat_pathways_stp_mapping as c
inner join amea.src_ext_txt_menat_pathways_arrivals_transit_mapping as p
ON c.distributor = p.sold_to_name
--where c.stp='6617100'	
) as sap
on (int(final.plant) = int(sap.plant) AND int(final.billto) = int(sap.sold_to_codes))
left join
(		--- ** Logic to get Confirmation Arrived Period Caluclated, above 1 month will be subtracted from Arrived period here **--		
		select case when ctr.load_date=current_date() then ctr.cycle
		else 
		src.month_no 
		end as ArrivedPeriod,month_no,
		case when ctr.load_date=current_date() then ctr.cycle_year
		else 
		src.financial_year 
		end as financial_year
		from 
		(select month_no,daate,financial_year
		from amea.pcd_ext_orc_kin_f_date 
		where daate=current_date()) src
		left join amea.src_ext_txt_menat_ksop_cycle_controllers ctr
		on src.daate = ctr.load_date	
) as dt
on 1=1