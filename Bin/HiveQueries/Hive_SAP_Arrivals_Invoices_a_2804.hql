SELECT 
  final.DocPeriod AS DocPeriod, 
  final.ReqDelDate ReqDelDate, 
  final.leaddate AS ShipmentDate, 
  concat("P", final.MONTH_NO) as ShipmentPeriod, 
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
  CASE WHEN (final.month_no + sap.arrival_values) <= 12 THEN final.financial_year
       WHEN (final.month_no + sap.arrival_values) > 12 THEN (final.financial_year + 1)
  END AS Year,
  CONCAT('P+', sap.arrival_values) as transittime,
  CASE WHEN (final.MONTH_NO + sap.arrival_values) <= 12 THEN (final.MONTH_NO + sap.arrival_values)
       WHEN (final.MONTH_NO + sap.arrival_values) > 12 THEN ((12 - (final.MONTH_NO + sap.arrival_values)) *-1)
  END AS ArrPeriod 
FROM 
  (
    SELECT 
      Concat('P', f.month_no) AS DocPeriod, 
      '' AS ReqDelDate, 
      f.FKDAT AS LeadDate, 
      f.matkl AS Category, 
      f.werks AS PlantCode, 
      c.txtmd AS PlantDesc, 
      f.aubel AS SalesDoc, 
      f.matnr AS Material, 
      d.txtmd AS MaterialDesc, 
      f.fkimg AS Cases, 
	  f.financial_year as financial_year,
      f.month_no AS MONTH_NO, 
      Regexp_replace(f.kunag, "^0+(?!$)", "") AS SoldTo, 
      Regexp_replace(f.matwa, "^0+(?!$)", "") AS DFU, 
      f.matnr AS SKU 
    FROM 
      (
        select 
          distinct fkdat, 
          matkl, 
          werks, 
          v1.aubel, 
          matnr, 
          fkimg, 
          kunag, 
          matwa,
		  dt.financial_year,
          dt.month_no 
        from 
          --amea.pcd_ext_orc_keu_2lis_13_vditm as 
		  (select * from amea.pcd_ext_orc_keu_2lis_13_vditm where vbtyp not in ('O','P','U'))  as v1
				  inner join (
        SELECT 
          AUBEL, 
          posnr, 
          Max(di_sequence_number) AS DI_SEQUENCE_NUMBER 
        FROM 
          ( 
			select a.* from amea.pcd_ext_orc_keu_2lis_13_vditm a
			inner join 
			(
			  	select AUBEL,posnr,max(load_dt) as max_load_date from amea.pcd_ext_orc_keu_2lis_13_vditm
			  	group by AUBEL,posnr
				) as b
			    on  a.AUBEL = b.AUBEL 
				AND a.load_dt = b.max_load_date 
				AND a.posnr = b.posnr
		) a			
        GROUP BY 
          AUBEL, 
          posnr
      ) AS MI ON MI.AUBEL = v1.AUBEL 
      AND MI.di_sequence_number = v1.di_sequence_number 
      AND MI.posnr = v1.posnr 
          inner join amea.pcd_ext_orc_kin_f_date as dt on dt.DAATE = v1.FKDAT 
         left  join
    (select MAX(DATE_ADD(month_start_date,-160)) as start_date from pcd_ext_orc_kin_f_date where daate = current_date ) as dt_max
    on 1 = 1
  WHERE dt.daate >= dt_max.start_date
		  
      ) as f 
      inner join (select distinct txtmd,WERKS from amea.pcd_ext_orc_keu_0plant_text) as c on c.WERKS = f.WERKS 
      inner join (select distinct MATNR,SPRAS,txtmd from amea.pcd_ext_orc_keu_0material_text) as d on d.MATNR = f.MATNR 
      and d.SPRAS = 'E' 
      left outer join (select distinct MATNR,MEINH from amea.pcd_ext_orc_keu_0mat_unit_attr) as e on e.MATNR = f.MATNR 
      and e.MEINH = 'KG'
  
  ) AS final
  inner join

(
select c.customer_code,c.sold_to_name,c.sold_to_codes,p.plant,p.arrival_values
from pcd_ext_orc_menat_pathway_arrivals_customer_country_from_sold_to_codes as c
inner join pcd_ext_orc_menat_pathway_arrivals_customer_country_from_plants as p
ON c.customer_code = p.customer_code
) as sap
on (int(final.PlantCode) = sap.plant AND final.SoldTo = sap.sold_to_codes)
left join
(
 select distinct sales_document
 from
  (
		select  sales_document  from 
		amea.uvw_ext_orc_menat_pathway_web_arrival_confirmation as b
		union
		select  sales_document from
		(
			select  sales_document from uvw_ext_orc_menat_pathway_web_arrival_no_confirmation
			where concat(int(plannedarrivalyear),lpad(int(plannedarrivalperiod),2,0))<concat(financial_year,lpad(substr(cycle,2),2,0))
			
		) as a
	) as web
) as web_confirm
on cast(web_confirm.sales_document as int)=cast(final.SalesDoc as int)
where web_confirm.sales_document is null