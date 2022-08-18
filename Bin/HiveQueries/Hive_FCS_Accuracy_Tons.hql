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
,tons_opening_stock	
,tons_p_1_forecast	
,tons_p_1_act	
,tons_p_1_delta	
,forecast_period	
,tons_p_1_fcst_accuracy	
,tons_p_1_fcst_accuracy_pct	
,tons_p_1_fcst_bias	
,for_sum
,actual_sum	
,tons_p_3_delta	
,tons_p_3_fcst_accuracy	
,tons_p_3_fcst_accuracy_pct	
,tons_p_3_fcst_bias
 from amea.uvw_FCS_ACCURACY_TONS
where 
 unix_timestamp(CONCAT(year,'-',period,'-01'), 'yyyy-MM-dd') >= 
unix_timestamp(date_add(last_day(add_months(current_date,-6)),1), 'yyyy-MM-dd')
AND
tons_p_1_forecast+tons_p_1_act+tons_opening_stock+for_sum+actual_sum > 0 ;