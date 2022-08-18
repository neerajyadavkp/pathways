# -*- coding: utf-8 -*-
import pyspark
from pyspark.sql import SparkSession

def transform_mep_data(spark):
    df = spark.sql("""SELECT total.distributorcode
 ,total.year
 ,total.quarter
 ,total.period
 ,total.bu_region
 ,total.category
 ,total.country_region
 ,total.country
 ,total.kelloggbrand
 ,total.target_volume as month_target,
 case
   when total.days_gone = 0 then 0
  else  total.days_gone / total.month_days
 end  as time_gone,
 CAST(total.daily_sales_volume as decimal(10,2)) as target_volume,
 CAST(case
  when total.target_volume=0 then 0
  else (total.daily_sales_volume / total.target_volume) 
 end as decimal(10,2)) as mtd_achived,
 CAST(case
  when (total.target_volume - total.daily_sales_volume) <0 then 0
  else (total.target_volume - total.daily_sales_volume)
 end as decimal(10,2)) as btg_volume,
 CAST(case 
  when total.remaining_days = 0 then 0
 when (total.target_volume - total.daily_sales_volume) <0 then 0
  else (total.target_volume - total.daily_sales_volume) / (total.remaining_days) 
 end AS DECIMAL (10,2))as btg_rr_required, 
 CAST(total.daily_sales_volume_lm as decimal(10,2)) as previous_month_ims,
 CAST(total.daily_sales_volume_lmtd as decimal(10,2)) as mtd_previous_month,
 CAST(sum(total.target_volume) over (partition by distributorcode, year, quarter, kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as decimal(10,2)) as quarter_target_volume,
 CAST(sum(total.daily_sales_volume) over (partition by distributorcode, year, quarter, kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as decimal(10,2)) as qtd_achived_volume,
 CAST(sum(total.target_volume - total.daily_sales_volume) over (partition by distributorcode, year, quarter, kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) / sum(total.target_volume) over (partition by distributorcode, year, quarter, kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) *100 as decimal(10,2)) as q_btg,
 CAST(sum(total.target_volume) over (partition by distributorcode, year, kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as decimal(10,2)) as ytd_target_volume,
 CAST(sum(total.daily_sales_volume) over (partition by distributorcode, year, kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as decimal(10,2)) as ytd_achived_volume,
 CAST(sum(total.target_volume - total.daily_sales_volume) over (partition by distributorcode, year, kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)/ sum(total.target_volume) over (partition by distributorcode, year, kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) *100 as decimal(10,2)) as y_btg,
 sum(total.remaining_days) over (partition by distributorcode, year,  kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as y_remaining_days,
 sum(total.remaining_days) over (partition by distributorcode, year, quarter, kelloggbrand order by period_no ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as q_remaining_days,
 total.month_days,
 total.days_gone
from
(
select 
  c.year,
  c.period,
  c.period_no,
  CASE
 WHEN c.period in ('P1','P2','P3') THEN 'Q01'
 WHEN c.period in ('P4','P5','P6') THEN 'Q02'
 WHEN c.period in ('P7','P8','P9') THEN 'Q03'
 WHEN c.period in ('P10','P11','P12') THEN 'Q04'
 ELSE 'err'
  END as quarter,
  c.kelloggbrand,
  c.distributorcode,
  sum(c.target_volume) as target_volume,
  sum(c.daily_sales_volume) as  daily_sales_volume,
  sum(c.daily_sales_volume_lm) as daily_sales_volume_lm,
  sum(c.daily_sales_volume_lmtd) as daily_sales_volume_lmtd,
  case 
  when c.period_no < month(current_date) then 0
 when c.period_no = month(current_date) then (datediff(last_day(current_date),current_date))
 when c.period_no > month(current_date) and c.period_no <10 then (datediff(last_day(concat(c.year,'-0',c.period_no,'-',01)),concat(c.year,'-0',c.period_no,'-',01))+1)
 else (datediff(last_day(concat(c.year,'-',c.period_no,'-',01)),concat(c.year,'-0',c.period_no,'-',01))+1)
  end as remaining_days,
  case 
 when c.period_no <10 then (datediff(last_day(concat(c.year,'-0',c.period_no,'-',01)),concat(c.year,'-0',c.period_no,'-',01))+1)
 else (datediff(last_day(concat(c.year,'-',c.period_no,'-',01)),concat(c.year,'-0',c.period_no,'-',01))+1)
  end as month_days,
  case 
  when c.period_no > month(current_date) then 0
 when c.period_no = month(current_date) then (datediff(current_date,trunc(current_date,'MM')))
 when c.period_no < month(current_date) and c.period_no <10 then (datediff(last_day(concat(c.year,'-0',c.period_no,'-',01)),concat(c.year,'-0',c.period_no,'-',01))+1)
 else (datediff(last_day(concat(c.year,'-',c.period_no,'-',01)),concat(c.year,'-0',c.period_no,'-',01))+1)
  end as days_gone,  
  c.category,
  c.country,
  c.country_region,
  c.bu_region
 from
(
select 
   cast(ims_daily.year as string) as year,
 concat('P',ims_daily.period) as period,
  cast(ims_daily.period as int) as period_no,
   upper(material.ims_brand) as kelloggbrand,
   upper(ims_daily.distributorcode) as distributorcode ,
   0.0 target_volume,
   sum(coalesce(ims_daily.salesincases,0) * coalesce(material.conversion_factor,0)) as daily_sales_volume,
  0.0 as daily_sales_volume_lm,
  0.0 as daily_sales_volume_lmtd,
   upper(material.category) as category,
  upper(customer.country) as country,
   upper(customer.country_region) as country_region,
   upper(customer.bu_region) as bu_region
from amea.pcd_ext_orc_menat_ims_actual_daily ims_daily  -- to be change to pcd_ext_orc_menat_ims_actual_daily when table is working
   inner join 
   (SELECT D.year, D.period, D.distributorcode, D.kelloggskucode, max(D.short_load_date) maxLoad_date
from amea.pcd_ext_orc_menat_ims_actual_daily D
group by D.year, D.period, D.distributorcode, D.kelloggskucode
) ims_daily_max 
ON ims_daily.year = ims_daily_max.year
AND ims_daily.period = ims_daily_max.period
AND upper(ims_daily.distributorcode) = upper(ims_daily_max.distributorcode)
AND upper(ims_daily.kelloggskucode) = upper(ims_daily_max.kelloggskucode)
AND ims_daily.short_load_date = ims_daily_max.maxLoad_date 
   inner join amea.pcd_ext_orc_menat_ims_material material 
   on (upper(material.sku) = upper(ims_daily.kelloggskucode)) 
      inner join (select distinct c.bu_region, c.country_region, c.country, c.customer_name
  from amea.pcd_ext_orc_menat_ims_customer c
  ) customer
   on (upper(customer.customer_name) = upper(ims_daily.distributorcode)) 
group by
   ims_daily.year,
   ims_daily.period,
   upper(material.ims_brand),
   upper(ims_daily.distributorcode),
  upper(material.category),
   upper(customer.country),
   upper(customer.country_region),
   upper(customer.bu_region)
union
 select 
   cast(ims_daily_i.year as string) as year,
 concat('P',ims_daily_i.period) as period,
  cast(ims_daily_i.period as int) as period_no,
   upper(material.market_segment) as kelloggbrand,
   upper(ims_daily_i.distributorcode) as distributorcode ,
   0.0 target_volume,
   sum(coalesce(ims_daily_i.salesincases,0) * coalesce(material.conversion_factor,0)) as daily_sales_volume,
  0.0 as daily_sales_volume_lm,
  0.0 as daily_sales_volume_lmtd,
   upper(material.category) as category,
  upper(customer.country) as country,
   upper(customer.country_region) as country_region,
   upper(customer.bu_region) as bu_region
from amea.pcd_ext_orc_menat_ims_actual_daily ims_daily_i  -- to be change to pcd_ext_orc_menat_ims_actual_daily when table is working
 inner join 
   (SELECT D.year, D.period, D.distributorcode, D.kelloggskucode, max(D.short_load_date) maxLoad_date
from amea.pcd_ext_orc_menat_ims_actual_daily D
group by D.year, D.period, D.distributorcode, D.kelloggskucode
) ims_daily_max 
ON ims_daily_i.year = ims_daily_max.year
AND ims_daily_i.period = ims_daily_max.period
AND upper(ims_daily_i.distributorcode) = upper(ims_daily_max.distributorcode)
AND upper(ims_daily_i.kelloggskucode) = upper(ims_daily_max.kelloggskucode)
AND ims_daily_i.short_load_date = ims_daily_max.maxLoad_date 
inner join amea.pcd_ext_orc_menat_ims_material material 
   on (upper(material.sku) = upper(ims_daily_i.kelloggskucode)) 
   and material.market_segment LIKE '%Innovation%'
  inner join    (select distinct c.bu_region, c.country_region, c.country, c.customer_name
  from amea.pcd_ext_orc_menat_ims_customer c
  ) customer
   on (upper(customer.customer_name) = upper(ims_daily_i.distributorcode)) 
group by
   ims_daily_i.year,
   ims_daily_i.period,
   upper(material.market_segment),
   upper(ims_daily_i.distributorcode),
  upper(material.category),
   upper(customer.country),
   upper(customer.country_region),
   upper(customer.bu_region)
union
select 
   cast(ims_daily_lmtd.year as string) as year,
  concat('P',cast(ims_daily_lmtd.period as int)+1) as period,
  cast(ims_daily_lmtd.period as int)+1 as period_no,
   upper(material.ims_brand) as kelloggbrand,
   upper(ims_daily_lmtd.distributorcode) as distributorcode,
   0.0 as target_volume,
   0.0 as daily_sales_volume,
  0.0 as daily_sales_volume_lm,
   sum(coalesce(ims_daily_lmtd.salesincases,0)* coalesce(material.conversion_factor,0)) as daily_sales_volume_lmtd,
  upper(material.category) as category,
   upper(customer.country) as country,
   upper(customer.country_region) as country_region,
   upper(customer.bu_region) as bu_region
from amea.pcd_ext_orc_menat_ims_actual_daily ims_daily_lmtd  -- to be change to pcd_ext_orc_menat_ims_actual_daily when table is working
    inner join 
   (SELECT D.year, D.period, D.distributorcode, D.kelloggskucode, max(D.short_load_date) maxLoad_date
from amea.pcd_ext_orc_menat_ims_actual_daily D
where D.short_load_date <= add_months(current_date,-1)
group by D.year, D.period, D.distributorcode, D.kelloggskucode
) ims_daily_max 
ON ims_daily_lmtd.year = ims_daily_max.year
AND ims_daily_lmtd.period = ims_daily_max.period
AND upper(ims_daily_lmtd.distributorcode) = upper(ims_daily_max.distributorcode)
AND upper(ims_daily_lmtd.kelloggskucode) = upper(ims_daily_max.kelloggskucode)
AND ims_daily_lmtd.short_load_date = ims_daily_max.maxLoad_date 
inner join amea.pcd_ext_orc_menat_ims_material material 
   on (upper(material.sku) = upper(ims_daily_lmtd.kelloggskucode)) 
   inner join 
   (select distinct c.bu_region, c.country_region, c.country, c.customer_name
  from amea.pcd_ext_orc_menat_ims_customer c
  ) customer
   on (upper(customer.customer_name) = upper(ims_daily_lmtd.distributorcode)) 
--where ims_daily_lmtd.short_load_date <= add_months(current_date,-1)
--where ims_daily_lmtd.period < month(current_date) -- to be replaced by above when loading date will be available in source
group by
   ims_daily_lmtd.year,
   ims_daily_lmtd.period,
   upper(material.ims_brand),
   upper(ims_daily_lmtd.distributorcode),
  upper(material.category),
   upper(customer.country),
   upper(customer.country_region),
   upper(customer.bu_region)
union
select 
   cast(ims_daily_lmtd_i.year as string) as year,
  concat('P',cast(ims_daily_lmtd_i.period as int)+1) as period,
  cast(ims_daily_lmtd_i.period as int)+1 as period_no,
   upper(material.market_segment) as kelloggbrand,
   upper(ims_daily_lmtd_i.distributorcode) as distributorcode,
   0.0 as target_volume,
   0.0 as daily_sales_volume,
  0.0 as daily_sales_volume_lm,
   sum(coalesce(ims_daily_lmtd_i.salesincases,0)* coalesce(material.conversion_factor,0)) as daily_sales_volume_lmtd,
  upper(material.category) as category,
   upper(customer.country) as country,
   upper(customer.country_region) as country_region,
   upper(customer.bu_region) as bu_region
from amea.pcd_ext_orc_menat_ims_actual_daily ims_daily_lmtd_i  -- to be change to pcd_ext_orc_menat_ims_actual_daily when table is working
     inner join 
   (SELECT D.year, D.period, D.distributorcode, D.kelloggskucode, max(D.short_load_date) maxLoad_date
from amea.pcd_ext_orc_menat_ims_actual_daily D
where D.short_load_date <= add_months(current_date,-1)
group by D.year, D.period, D.distributorcode, D.kelloggskucode
) ims_daily_max 
ON ims_daily_lmtd_i.year = ims_daily_max.year
AND ims_daily_lmtd_i.period = ims_daily_max.period
AND upper(ims_daily_lmtd_i.distributorcode) = upper(ims_daily_max.distributorcode)
AND upper(ims_daily_lmtd_i.kelloggskucode) = upper(ims_daily_max.kelloggskucode)
AND ims_daily_lmtd_i.short_load_date = ims_daily_max.maxLoad_date 
inner join amea.pcd_ext_orc_menat_ims_material material 
   on upper(material.sku) = upper(ims_daily_lmtd_i.kelloggskucode)
   and material.market_segment LIKE '%Innovation%'
   inner join 
   (select distinct c.bu_region, c.country_region, c.country, c.customer_name
  from amea.pcd_ext_orc_menat_ims_customer c
  ) customer
   on (upper(customer.customer_name) = upper(ims_daily_lmtd_i.distributorcode)) 
--where ims_daily_lmtd_i.short_load_date <= add_months(current_date,-1)
--where ims_daily_lmtd_i.period < month(current_date) -- to be replaced by above when loading date will be available in source
group by
   ims_daily_lmtd_i.year,
   ims_daily_lmtd_i.period,
   upper(material.market_segment),
   upper(ims_daily_lmtd_i.distributorcode),
  upper(material.category),
   upper(customer.country),
   upper(customer.country_region),
   upper(customer.bu_region)
union
select 
   cast(ims_daily_lm.year as string) as year,
  concat('P',cast(ims_daily_lm.period as int)+1) as period,
  cast(ims_daily_lm.period as int)+1 as period_no,
   upper(material.ims_brand) as kelloggbrand,
   upper(ims_daily_lm.distributorcode) as distributorcode,
   0.0 as target_volume,
   0.0 as daily_sales_volume,
   sum(coalesce(ims_daily_lm.salesincases,0)* coalesce(material.conversion_factor,0)) as daily_sales_volume_lm,
  0.0 as daily_sales_volume_lmtd,
  upper(material.category) as category,
   upper(customer.country) as country,
   upper(customer.country_region) as country_region,
   upper(customer.bu_region) as bu_region
from amea.pcd_ext_orc_menat_ims_actual_daily ims_daily_lm  -- to be change to archive_ext_txt_menat_ims_actual_daily when table is working
     inner join 
   (SELECT D.year, D.period, D.distributorcode, D.kelloggskucode, max(D.short_load_date) maxLoad_date
from amea.pcd_ext_orc_menat_ims_actual_daily D
group by D.year, D.period, D.distributorcode, D.kelloggskucode
) ims_daily_max 
ON ims_daily_lm.year = ims_daily_max.year
AND ims_daily_lm.period = ims_daily_max.period
AND upper(ims_daily_lm.distributorcode) = upper(ims_daily_max.distributorcode)
AND upper(ims_daily_lm.kelloggskucode) = upper(ims_daily_max.kelloggskucode)
AND ims_daily_lm.short_load_date = ims_daily_max.maxLoad_date 
   inner join amea.pcd_ext_orc_menat_ims_material material 
   on (upper(material.sku) = upper(ims_daily_lm.kelloggskucode)) 
      inner join    (select distinct c.bu_region, c.country_region, c.country, c.customer_name
  from amea.pcd_ext_orc_menat_ims_customer c
  ) customer
   on (upper(customer.customer_name) = upper(ims_daily_lm.distributorcode))
where ims_daily_lm.period < month(current_date)
group by
   ims_daily_lm.year,
   ims_daily_lm.period,
   upper(material.ims_brand),
   upper(ims_daily_lm.distributorcode),
  upper(material.category),
   upper(customer.country),
   upper(customer.country_region),
   upper(customer.bu_region)
union
select 
   cast(ims_daily_lm_i.year as string) as year,
  concat('P',cast(ims_daily_lm_i.period as int)+1) as period,
  cast(ims_daily_lm_i.period as int)+1 as period_no,
   upper(material.market_segment) as kelloggbrand,
   upper(ims_daily_lm_i.distributorcode) as distributorcode,
   0.0 as target_volume,
   0.0 as daily_sales_volume,
   sum(coalesce(ims_daily_lm_i.salesincases,0)* coalesce(material.conversion_factor,0)) as daily_sales_volume_lm,
  0.0 as daily_sales_volume_lmtd,
  upper(material.category) as category,
   upper(customer.country) as country,
   upper(customer.country_region) as country_region,
   upper(customer.bu_region) as bu_region
from amea.pcd_ext_orc_menat_ims_actual_daily ims_daily_lm_i  -- to be change to archive_ext_txt_menat_ims_actual_daily when table is working
      inner join 
   (SELECT D.year, D.period, D.distributorcode, D.kelloggskucode, max(D.short_load_date) maxLoad_date
from amea.pcd_ext_orc_menat_ims_actual_daily D
group by D.year, D.period, D.distributorcode, D.kelloggskucode
) ims_daily_max 
ON ims_daily_lm_i.year = ims_daily_max.year
AND ims_daily_lm_i.period = ims_daily_max.period
AND upper(ims_daily_lm_i.distributorcode) = upper(ims_daily_max.distributorcode)
AND upper(ims_daily_lm_i.kelloggskucode) = upper(ims_daily_max.kelloggskucode)
AND ims_daily_lm_i.short_load_date = ims_daily_max.maxLoad_date 
inner join amea.pcd_ext_orc_menat_ims_material material 
   on (upper(material.sku) = upper(ims_daily_lm_i.kelloggskucode)) 
   and material.market_segment LIKE '%Innovation%'
      inner join (select distinct c.bu_region, c.country_region, c.country, c.customer_name
  from amea.pcd_ext_orc_menat_ims_customer c
  ) customer
   on (upper(customer.customer_name) = upper(ims_daily_lm_i.distributorcode)) 
 where ims_daily_lm_i.period < month(current_date)
group by
   ims_daily_lm_i.year,
   ims_daily_lm_i.period,
   upper(material.market_segment),
   upper(ims_daily_lm_i.distributorcode),
  upper(material.category),
   upper(customer.country),
   upper(customer.country_region),
   upper(customer.bu_region)
union
select 
   cast(target.year as string) as year,
   target.period,
  cast(regexp_replace(target.period, 'P', '') as int) as period_no,
   upper(target.kelloggbrand) as kelloggbrand,
   upper(target.distributorcode) as distributorcode,
   sum(coalesce(target.volume,0)) as target_volume,--sum(coalesce(target.volume,0)) as target_volume,
   0.0 as daily_sales_volume,
  0.0 as daily_sales_volume_lm,
  0.0 as daily_sales_volume_lmtd,
  upper(m_brand.category) as category,
   upper(customer.country) as country,
   upper(customer.country_region) as country_region,
   upper(customer.bu_region) as bu_region
from amea.pcd_ext_orc_menat_ims_target target 
   inner join
   (select distinct c.bu_region, c.country_region, c.country, c.customer_name
  from amea.pcd_ext_orc_menat_ims_customer c
  ) customer
   on (upper(trim(customer.customer_name)) = upper(trim(target.distributorcode))) 
  inner join 
  (
 select distinct
   m.kelloggbrand,
   first_value(m.category) over (partition by m.kelloggbrand order by m.no_sku desc) as category
  from 
  (
   select count(sku) no_sku,
  material.ims_brand as kelloggbrand,
  upper(material.category) as category
   from amea.pcd_ext_orc_menat_ims_material material 
  group by 
   material.ims_brand,
  upper(material.category)
   ) m
  ) m_brand
   on (upper(trim(m_brand.kelloggbrand)) = upper(trim(target.kelloggbrand)))
group BY
   target.year,
   target.period,
   upper(target.kelloggbrand),
   upper(target.distributorcode),
  upper(m_brand.category),
   upper(customer.country),
   upper(customer.country_region),
   upper(customer.bu_region)
union
select 
   cast(target_i.year as string) as year,
   target_i.period,
  cast(regexp_replace(target_i.period, 'P', '') as int) as period_no,
   upper(m_brand.kelloggbrand) as kelloggbrand,
   upper(target_i.distributorcode) as distributorcode,
   sum(coalesce(target_i.volume,0)) as target_volume,--sum(coalesce(target_i.volume,0)) as target_volume,
   0.0 as daily_sales_volume,
  0.0 as daily_sales_volume_lm,
  0.0 as daily_sales_volume_lmtd,
  upper(m_brand.category) as category,
   upper(customer.country) as country,
   upper(customer.country_region) as country_region,
   upper(customer.bu_region) as bu_region
from amea.pcd_ext_orc_menat_ims_target target_i
   inner join
   (select distinct c.bu_region, c.country_region, c.country, c.customer_name
  from amea.pcd_ext_orc_menat_ims_customer c
  ) customer
   on (upper(trim(customer.customer_name)) = upper(trim(target_i.distributorcode))) 
  inner join 
  (
 select distinct
   m.kelloggbrand,
   first_value(m.category) over (partition by m.kelloggbrand order by m.no_sku desc) as category
  from 
  (
   select count(sku) no_sku,
  material.market_segment as kelloggbrand,
  upper(material.category) as category
   from amea.pcd_ext_orc_menat_ims_material material 
   where material.market_segment LIKE '%Innovation%'
  group by 
   material.ims_brand,
    material.market_segment,
  upper(material.category)
   ) m
  ) m_brand
   on (upper(trim(m_brand.kelloggbrand)) = upper(trim(target_i.kelloggbrand)))
group BY
   target_i.year,
   target_i.period,
   upper(m_brand.kelloggbrand),
   upper(target_i.distributorcode),
  upper(m_brand.category),
   upper(customer.country),
   upper(customer.country_region),
   upper(customer.bu_region)
) c
group BY
  case 
  when c.period_no < month(current_date) then 0
 when c.period_no = month(current_date) then (datediff(last_day(current_date),current_date))
 when c.period_no > month(current_date) and c.period_no <10 then (datediff(last_day(concat(c.year,'-0',c.period_no,'-',01)),concat(c.year,'-0',c.period_no,'-',01))+1)
 else (datediff(last_day(concat(c.year,'-',c.period_no,'-',01)),concat(c.year,'-0',c.period_no,'-',01))+1)
  end,
  c.year,
  c.period,
  c.period_no,
  CASE
  WHEN c.period in ('P1','P2','P3') THEN 'Q01'
 WHEN c.period in ('P4','P5','P6') THEN 'Q02'
 WHEN c.period in ('P7','P8','P9') THEN 'Q03'
 WHEN c.period in ('P10','P11','P12') THEN 'Q04'
 ELSE 'err'
  END,
  c.kelloggbrand,
  c.distributorcode,
  c.category,
  c.country,
  c.Country_Region,
  c.bu_region
order by c.distributorcode, c.year, period_no, c.bu_region, c.kelloggbrand
) total
""").cache()
    df.coalesce(1).write.format("csv").option("header", True).mode('overwrite').option('sep', ',').save('hdfs:///work/kap/menat_dsr/csv')


if __name__ == "__main__":
    print(pyspark.__version__)
    spark = SparkSession.builder.master("yarn").config("spark.executor.instances","10").config("spark.driver.cores","4").config("spark.executor.cores","2").enableHiveSupport().getOrCreate()
    transform_mep_data(spark)
    spark.stop()
