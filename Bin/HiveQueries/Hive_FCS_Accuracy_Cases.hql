select 
year	
,period	
,dfunumber
,distributorcode
,brand
,dfu	
,category	
,version	
,conversion	
,cases_opening_stock	
,cases_p_1_forecast	
,cases_p_1_act	
,cases_p_1_delta	
,forecast_period	
,cases_p_1_fcst_accuracy	
,cases_p_1_fcst_accuracy_pct	
,cases_p_1_fcst_bias	
,for_sum
,actual_sum	
,cases_p_3_delta	
,cases_p_3_fcst_accuracy	
,cases_p_3_fcst_accuracy_pct	
,cases_p_3_fcst_bias 
from amea.uvw_FCS_ACCURACY_CASES
where 
unix_timestamp(CONCAT(year,'-',period,'-01'), 'yyyy-MM-dd') >= 
unix_timestamp(date_add(last_day(add_months(current_date,-6)),1), 'yyyy-MM-dd')
AND
cases_p_1_forecast+cases_p_1_act+cases_opening_stock+for_sum+actual_sum > 0;