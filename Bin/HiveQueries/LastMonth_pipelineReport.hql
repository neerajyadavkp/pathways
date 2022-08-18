select 
tableau.year as year,tableau.quarter as quarter,tableau.month as month,tableau.period as period,tableau.yearmonth as yearmonth,tableau.cycle as cycle,
tableau.distributorcode as distributorcode,tableau.distributorname as distributorname,tableau.country as country,tableau.sku as sku,tableau.skudescription as skudescription,
tableau.brand as brand,tableau.category as category,tableau.version as version,tableau.country_group as country_group,
tableau.conversion_factor as conversion_factor,tableau.os as os,tableau.arr as arr,tableau.ims as ims,tableau.woc as woc,tableau.adj as adj,tableau.flag as flag,tableau.load_date as load_date

from amea.pcd_ext_orc_menat_pathway_drm_pipelineinput as tableau
JOIN
(
	select t.version,ypdt.prevyear, ypdt.prevperiod from amea.pcd_ext_orc_menat_pathway_drm_pipelineinput as t
	JOIN
	(
		select yrmon.prevyear,yrmon.prevperiod,dt.max_load_dt from 
		(
		   select  
		CASE WHEN (MaxPeriod - 1) = 0 THEN (Y.MaxYear - 1) ELSE Y.MaxYear
			 END as PrevYear,
		CASE WHEN (MaxPeriod - 1) = 0 THEN CONCAT('P', '12') ELSE CONCAT('P', CAST((MaxPeriod - 1) as int))
			 END as PrevPeriod	 
		from
		(select MAX(year)as MaxYear from amea.pcd_ext_orc_menat_pathway_ims_forecast) Y
		JOIN (select MAX(period) as MaxPeriod from amea.pcd_ext_orc_menat_pathway_ims_forecast) M
		on 1=1
		) yrmon 
		JOIN
		(
			select MAX(tableau.load_date) as max_load_dt from
			amea.pcd_ext_orc_menat_pathway_drm_pipelineinput as tableau
			join 
			(
			  select  
			CASE WHEN (MaxPeriod - 1) = 0 THEN (Y.MaxYear - 1) ELSE Y.MaxYear
				 END as PrevYear,
			CASE WHEN (MaxPeriod - 1) = 0 THEN CONCAT('P', '12') ELSE CONCAT('P', CAST((MaxPeriod - 1) as int))
				 END as PrevPeriod	 
			from
			(select MAX(year)as MaxYear from amea.pcd_ext_orc_menat_pathway_ims_forecast) Y
			JOIN (select MAX(period) as MaxPeriod from amea.pcd_ext_orc_menat_pathway_ims_forecast) M
			on 1=1
			)YM
			on tableau.year = YM.prevyear AND tableau.period = YM.prevperiod 
		) dt
		ON 1=1
	) ypdt
	ON t.year = ypdt.prevyear AND t.period = ypdt.prevperiod
	WHERE t.load_date = ypdt.max_load_dt
	LIMIT 1
) ver_yr_p
ON 1=1
WHERE tableau.year = ver_yr_p.prevyear AND tableau.period = ver_yr_p.prevperiod  AND tableau.version = ver_yr_p.version
;
