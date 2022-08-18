select final.salesref as salesref
,final.createdon as createdon
,final.reqdeldate as reqdeldate
,final.soldto as soldto
,final.shipto as shipto
,final.customernamesoldto as customernamesoldto
,final.purchaseorderno as purchaseorderno
,final.shpt as shpt
,final.material as material
,final.materialdesc as materialdesc
,final.matkl as Category
,final.salesorg as salesorg
,final.netweight as netweight
,final.weightunit as weightunit
,final.cases as cases
,final.pallets as pallets
,final.market as market
,final.country as country
,final.transit_time as transit_time
,final.shipment_date as shipment_date,
dt_forship.display_month_no as shipment_period
from(
SELECT DISTINCT 
b.VBELN as SalesRef,
b.ERDAT as CreatedOn,
b.VDATU as ReqDelDate,
--b.KUNAG as SoldTo,
b.KUNNR as SoldTo,
c.ZZSHIPTO as ShipTo,
c1.TXTMD as CustomerNameSoldTo,
'' as PurchaseOrderNO,
a.WERKS as ShPt,
a.matkl,
a.MATNR as Material,
d.TXTMD as MaterialDesc,
b.VKORG as SalesOrg,
a.NTGEW as NetWeight,
a.GEWEI as WeightUnit,
a.KWMENG as Cases,
round((e.UMREN/e.UMREZ) * a.KBMENG,0) as Pallets,
sap_transit.region as Market ,sap_transit.country_in_sap as country,
sap_transit.arrival_values as transit_time,
Date_add(b.vdatu, int((sap_transit.arrival_values))*-1) as shipment_date
FROM (
	select * from amea.pcd_ext_orc_keu_2lis_11_vaitm
	where (abgru==" " or abgru=="" or abgru is null) and (rocancel<>'r') and (auart<>'G2') --and vbeln='0013478113'
)as a 
inner join
 (
        SELECT 
          vbeln, 
          posnr, 
          Max(di_sequence_number) AS DI_SEQUENCE_NUMBER 
        FROM 
          (
		select a.* from amea.pcd_ext_orc_keu_2lis_11_vaitm a 
		inner join
		(
			select vbeln,posnr,max(load_dt) as max_load_dt from amea.pcd_ext_orc_keu_2lis_11_vaitm group by vbeln,posnr
		) b
		on a.vbeln=b.vbeln and a.posnr=b.posnr and b.max_load_dt=a.load_dt
        ) a 
	GROUP BY vbeln, posnr
) AS MI ON MI.vbeln = a.vbeln 
AND MI.di_sequence_number = a.di_sequence_number 
AND MI.posnr = a.posnr 
inner join 
(
                select distinct v1.VBELN,ERDAT,VDATU,KUNNR,VKORG from amea.pcd_ext_orc_keu_2lis_11_vahdr v1				
				inner join (
							SELECT 
							  vbeln, 
							  Max(di_sequence_number) AS DI_SEQUENCE_NUMBER 
							FROM 
							    ( 
									select b.* from amea.pcd_ext_orc_keu_2lis_11_vahdr b
									inner join 
									(select max(load_dt) as max_load_dt, vbeln from pcd_ext_orc_keu_2lis_11_vahdr group by vbeln) c
									on b.vbeln=c.vbeln and b.load_dt=c.max_load_dt
								)b
							GROUP BY 
							  vbeln
						  ) AS MH ON MH.vbeln = v1.vbeln
							AND MH.DI_SEQUENCE_NUMBER = v1.di_sequence_number 						  
) as b on b.VBELN=a.VBELN 
inner join (select distinct KUNNR,ZZSHIPTO from amea.pcd_ext_orc_keu_0customer_attr) as c on c.KUNNR=b.KUNNR --
inner join (select distinct KUNNR,TXTMD from amea.pcd_ext_orc_keu_0customer_text) as c1 on c1.KUNNR=c.KUNNR  --
inner join (
			select distinct a.MATNR,a.TXTMD,a.SPRAS from amea.pcd_ext_orc_keu_0material_text a
			inner join
				(
					select MATNR,max(load_dt) as max_load_dt from amea.pcd_ext_orc_keu_0material_text
					group by MATNR
				)b
				on a.MATNR=b.MATNR and a.load_dt=b.max_load_dt
				
			)  as d on d.MATNR=a.MATNR and d.SPRAS='E'  --
left outer join (select distinct  MATNR,MEINH,UMREN,UMREZ from  pcd_ext_orc_keu_0mat_unit_attr) as e on e.MATNR=a.MATNR and e.MEINH='PAL' --  pcd_ext_orc_keu_0mat_unit_attr
inner join
                  (
                                select ct.customer_code,ct.sold_to_name,ct.sold_to_codes,pt.plant,pt.arrival_values,ct.region,ct.country_in_sap
                                from pcd_ext_orc_menat_pathway_customer_country_transit_times_sold_to_codes as ct
                                inner join pcd_ext_orc_menat_pathway_customer_country_transit_times_plants as pt
                                ON ct.customer_code = pt.customer_code
                  ) AS sap_transit ON int(a.werks) = sap_transit.plant AND (Regexp_replace(b.kunnr, "^0+(?!$)", "")) = sap_transit.sold_to_codes
inner join amea.pcd_ext_orc_kin_f_date AS dt ON dt.daate = b.erdat
inner  join
    (select MAX(DATE_ADD(month_start_date,-160)) as start_date from pcd_ext_orc_kin_f_date where daate = current_date ) as dt_max
    on 1 = 1
  WHERE dt.daate >= dt_max.start_date
) final
left join
amea.pcd_ext_orc_kin_f_date dt_forship
on dt_forship.daate=final.shipment_date
left join amea.pcd_ext_orc_keu_2lis_13_vditm as INV on INV.AUBEL = final.SalesRef
where INV.AUBEL is null