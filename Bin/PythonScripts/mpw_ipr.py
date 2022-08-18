# -*- coding: utf-8 -*-
"""
Date       : 22/02/2021
Author     : Vibha Goswami, Ananda and Neeraj
Description: This script combines IMS , OS, Shipment Arrivals and create the calculated Facts Suggested Orders, WOC, and ADJ.
Project    : MENAT Pathways - Phase 1
Region     : AMEA
"""
import math
import datetime
from datetime import datetime as dt
import pandas as pd
import numpy as np
import os
pd.options.mode.chained_assignment = None

# Remove this code, this is just to view data for testing purpose in pycharm
# pd.set_option('display.width', 300)
# pd.set_option('display.max_columns', 20)

from dotenv import load_dotenv
load_dotenv()
print(os.environ)

# Setting one_time_upload variable to 1 when it's the first time go live. For next cycles, change this variable value to zero

one_time_upload = 1



# Setting folder path and filename variable 
filepath = os.environ.get('filepath')
# filepath = 'C:/Users/M1044306/Documents/Menat Ksop Data/Feb Menat UAT/'
forecast_filename = os.environ.get('forecast_filename')
actuals_filename = os.environ.get('actuals_filename')
os_filename = os.environ.get('os_filename')
web_filename = os.environ.get('web_filename')
adj_filename = os.environ.get('adj_filename')
sap_filename = os.environ.get('sap_filename')
stock_cover_days_filename = os.environ.get('stock_cover_days_filename')
dfu_mapping_filename = os.environ.get('dfu_mapping_filename')
customer_country_filename = os.environ.get('customer_country_filename')
last_month_pipe_filename = os.environ.get('last_month_pipe_filename')
leftover_filename = os.environ.get('leftover_output')
python_csv_output = os.environ.get('python_csv_output')
python_excel_output = os.environ.get('python_excel_output')
adj_output = os.environ.get('adj_output')


##** Function to convert SKU to DFU except for few plants **# 
def convert_sku_to_dfu(plant, DFU):    
    #print(region)
    #if (system == 'EAP') & ((region).upper()!='SOUTH AFRICA'):
    if (plant not in [5919,5920,6900,6902,6903,344,339,370]):
        DFU = str(DFU)[:-3]+'000'
        return DFU
    else:
        return str(DFU)

# Checking if all the files are present, if not raise an exception and exit
if not os.path.exists(os.path.join(filepath,forecast_filename)):
    raise Exception("Forecast file not present. Quiting further processing...")
    exit(-1)
elif not os.path.exists(os.path.join(filepath,actuals_filename)):
    raise Exception("Actual file not present. Quiting further processing...")
    exit(-1)
elif not os.path.exists(os.path.join(filepath,os_filename)):
    raise Exception("OS file not present. Quiting further processing...")
    exit(-1)
elif not os.path.exists(os.path.join(filepath,web_filename)):
    raise Exception("Web file not present. Quiting further processing...")
    exit(-1)
elif not os.path.exists(os.path.join(filepath,adj_filename)):
    raise Exception("ADJ file not present. Quiting further processing...")
    exit(-1)
elif not os.path.exists(os.path.join(filepath,sap_filename)):
    raise Exception("SAP file not present. Quiting further processing...")
    exit(-1)
elif not os.path.exists(os.path.join(filepath,stock_cover_days_filename)):
    raise Exception("Stock cover days mapping file not present. Quiting further processing...")
    exit(-1)
elif not os.path.exists(os.path.join(filepath,dfu_mapping_filename)):
    raise Exception("DFU mapping file not present. Quiting further processing...")
    exit(-1)
elif not os.path.exists(os.path.join(filepath,customer_country_filename)):
    raise Exception("Customer country file not present. Quiting further processing...")
    exit(-1)
#########################
# Reading the Billto to Customer name  mapping
#########################
df_cust_map = pd.read_csv(os.path.join(filepath,customer_country_filename))

#####################
# Reading DFU Mapping File
#####################
df_dfu = pd.read_csv(os.path.join(filepath,dfu_mapping_filename))
# print(df_dfu)
df_dfu['NEW DFU'] = df_dfu['NEW DFU'].astype(str)
df_dfu['OLD DFU'] = df_dfu['OLD DFU'].astype(str)

#########################
#  Reading forecast data
#########################
df_f = pd.read_csv(os.path.join(filepath,forecast_filename))

df_f = df_f.rename({'distributorcode': 'DistributorCode', 'dfucode': 'DFU', 'dfudescription': 'DFUDescription',
                    'kelloggbrand': 'Brand', 'sales': 'IMS', 'version': 'Version', 'year': 'Year', 'load_date': 'Load_date',
                    'period': 'Period'}, axis=1)
df_f['DFU'] = df_f['DFU'].astype(str)
dfm = pd.merge(df_f, df_dfu[['NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['NEW DFU'] )
# print(dfm)
dfm2 = pd.merge(dfm, df_dfu[['OLD DFU', 'NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['OLD DFU'] )

dfm2['Changed DFU'] = np.where(
    (dfm2['NEW DFU_x'].isnull()),
    (dfm2['NEW DFU_y']), dfm2['NEW DFU_x']
)
#dfm2.to_csv('dfm2_test2.csv')
df_f=dfm2[['Year', 'Period', 'yearperiod', 'DistributorCode', 'Changed DFU','Version','IMS','Load_date']] #'DFUDescription', 'Brand','IMS']]
df_f['Changed DFU'] = df_f['Changed DFU'].fillna(0).astype(str)
df_dfu['NEW DFU'] = df_dfu['NEW DFU'].astype(str)
df_f=pd.merge(df_f, df_dfu[['NEW DFU','NEW DESC','Brand']].drop_duplicates(), how='left', left_on=['Changed DFU'], right_on=['NEW DFU'] )

df_f = df_f.rename({'Changed DFU': 'DFU','NEW DESC':'DFUDescription'}, axis=1)

# Find maximum load_date
load_dt = df_f['Load_date']
max_load_dt = load_dt.max()

#df_f = df_f[df_f['Load_date'] == max_load_dt] ## Commented by Neeraj, Not needed

# Putting null IMS as zero so that it comes in group by
df_f['IMS'] = df_f['IMS'].fillna(0)

# Changing datatype of year
df_f['Year'] = df_f['Year'].astype('int64')
df_f['Period'] = df_f['Period'].astype('int64')
df_f['DistributorCode'] = df_f['DistributorCode'].astype(str)
df_f['DFU'] = df_f['DFU'].astype(str)

# Group by and sum to remove duplicates
df_f = df_f.groupby(['Year', 'Period', 'yearperiod', 'DistributorCode', 'DFU', 'DFUDescription', 'Brand', 'Version'], as_index=False)['IMS'].sum()

# Checking if forecast file is empty, if yes, exit the process
if df_f.empty:
    raise Exception("Forecast file is empty. Exiting the process")
    exit(-1)

# Calculate current yearmonth (to be used for filtering forecast data from the current month)
yr = df_f['Year']
curr_yr = yr.max()
p = df_f['Period']
curr_p = p.max()
curr_yearmonth = str(curr_yr)+str(curr_p).zfill(2)

# Calculate Quarter,Month,Period,Cycle
quarter = "Q"+str(int(math.ceil(float(curr_p)/3)))
month = dt.strptime(str(curr_p), "%m").strftime("%b")
str_period = "P"+str(curr_p)
cycle = str(curr_p)+"+"+str(12-curr_p)

# Adding quarter, month, period and cycle in df_f in the format of tableau report
df_f['Quarter'] = quarter
df_f['Month'] = month
df_f['Cycle'] = cycle
df_f = df_f.drop('Period', 1)
df_f['Period'] = str_period

# Filter forecast data to remove previous month data
df_f['yearperiod_year'] = df_f['yearperiod'].str[-4:]
df_f['yearperiod_period'] = df_f['yearperiod'].str[:-4]
df_f['yearperiod_period'] = df_f['yearperiod_period'].str.replace('P', '')
df_f['yearperiod_period'] = df_f['yearperiod_period'].str.zfill(2)
df_f['YearMonth'] = df_f['yearperiod_year']+df_f['yearperiod_period']
df_f = df_f.drop('yearperiod_period', 1)
df_f = df_f.drop('yearperiod_year', 1)
df_f = df_f[df_f['YearMonth'] >= curr_yearmonth]

# Reformat forecast data for tableau format
df_f = df_f[['Year', 'Quarter', 'Month', 'Period', 'YearMonth', 'Cycle', 'DistributorCode', 'DFU', 'DFUDescription', 'Brand', 'Version', 'IMS']]

# Getting forecast version
version = df_f['Version'].max()

### Getting the list of distributor for Which Pipeline is running
dsr_list=df_f['DistributorCode'].unique().tolist()
dsr_list_for_filter=[x.upper() for x in dsr_list]

# Calculating how many months data is available
total_no_of_months = df_f['YearMonth'].nunique()
# print(total_no_of_months)


# Creating output filenames as per naming convention to add period and version
python_csv_output = python_csv_output #+str(version) #+'_P'+str(curr_p)+'.csv'
python_excel_output = python_excel_output #+str(version) #+'_P'+str(curr_p)+'.xlsx'
adj_output = adj_output #+'.csv'



########################
#  Reading actuals data
########################
df_a = pd.read_csv(os.path.join(filepath,actuals_filename))

df_a = df_a.rename({'distributorcode': 'DistributorCode', 'dfucode': 'DFU', 'dfudescription': 'DFUDescription',
                    'kelloggbrand': 'Brand', 'sales': 'IMS', 'year': 'Year', 'period': 'Period'}, axis=1)

## Filtering the Actual data only for the Distributor for which latest Forecast file is uploaded 
df_a=df_a[df_a['DistributorCode'].str.upper().isin(dsr_list_for_filter)]
# Changing datatype
df_a['Year'] = df_a['Year'].astype('int64')
df_a['Period'] = df_a['Period'].astype('int64')
df_a['DistributorCode'] = df_a['DistributorCode'].astype(str)
df_a['DFU'] = df_a['DFU'].astype(str)

## filtering Actual data to be less than that of Forecast data period for which process is running
df_a['YearPeriod']=df_a['Year'].astype('str')+df_a['Period'].astype('str').str.zfill(2)
df_a = df_a[df_a['YearPeriod'].astype('int64') < int(curr_yearmonth)]

#####################
# Maping the OLD DFU to New DFU for Historical Actual data to backtrack OLD DFUs
#####################

dfm = pd.merge(df_a, df_dfu[['NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['NEW DFU'] )
# print(dfm)
dfm2 = pd.merge(dfm, df_dfu[['OLD DFU', 'NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['OLD DFU'] )

### Consider NEW DFU if it present else consider the NEW DFU mapped against OLD DFU
dfm2['Changed DFU'] = np.where(
    (dfm2['NEW DFU_x'].isnull()),
    (dfm2['NEW DFU_y']), dfm2['NEW DFU_x']
)

df_a=dfm2[['Year', 'Period', 'DistributorCode', 'Changed DFU', 'IMS']] #'DFUDescription', 'Brand','IMS']]

df_a['Changed DFU'] = df_a['Changed DFU'].astype(str)

## Joined again with DFU Mapping to get the latest DFU Brand and  description

df_a=pd.merge(df_a, df_dfu[['NEW DFU','NEW DESC','Brand']].drop_duplicates(), how='left', left_on=['Changed DFU'], right_on=['NEW DFU'] )
df_a = df_a.rename({'Changed DFU': 'DFU','NEW DESC':'DFUDescription'}, axis=1)
df_a=df_a[['Year', 'Period', 'DistributorCode', 'DFU','DFUDescription', 'Brand', 'IMS']] #'DFUDescription', 'Brand','IMS']]

#df_a['DFU'] = df_a['DFU'].astype(str)
# Group by and sum to remove duplicates
df_a = df_a.groupby(['Year', 'Period', 'DistributorCode', 'DFU', 'DFUDescription', 'Brand'], as_index=False)['IMS'].sum()

if df_a.empty:
    print("No actual data")
    df_a = pd.read_csv(filepath + actuals_filename)
    df_a = df_a.rename({'distributorcode': 'DistributorCode', 'dfucode': 'DFU', 'dfudescription': 'DFUDescription',
                        'kelloggbrand': 'Brand', 'sales': 'IMS', 'year': 'Year', 'period': 'Period'}, axis=1)


# Calculating YearMonth, Period, Quarter, period column for actuals
df_a['Period'] = df_a['Period'].astype(str)
df_a['YearMonth'] = df_a['Year'].apply(str) + df_a['Period'].str.zfill(2)
df_a['Period'] = 'P'+str(curr_p)
df_a['Quarter'] = quarter
df_a['Month'] = month
df_a['Cycle'] = cycle


# Reformat actual data for tableau format

df_a = df_a[['Year', 'Quarter', 'Month', 'Period', 'YearMonth', 'Cycle', 'DistributorCode', 'DFU', 'DFUDescription', 'Brand', 'IMS']]

# Add version column to actuals
df_a['Version'] = version

##################################
# Combine actual and forecast data
##################################
df = df_a.append(df_f, ignore_index=True)
df['DistributorCode']=df['DistributorCode'].str.upper()

# Filter rows where DFU or Distributor code is null
df = df[df['DFU'].notnull()]
df = df[df['DistributorCode'].notnull()]


#########################
# Reading Stock data
#########################
df_o = pd.read_csv(os.path.join(filepath,os_filename))

df_o = df_o.rename({'dfucode': 'DFU', 'volume': 'OS', 'year': 'Year', 'period': 'Period', 'distributorcode': 'DistributorCode'}, axis=1)

df_o['DFU'] = df_o['DFU'].astype(str)

#####################
# Maping the OLD DFU to New DFU for Historical Actual data to backtrack OLD DFUs
#####################

dfm = pd.merge(df_o, df_dfu[['NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['NEW DFU'] )
# print(dfm)
dfm2 = pd.merge(dfm, df_dfu[['OLD DFU', 'NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['OLD DFU'] )

dfm2['Changed DFU'] = np.where(
    (dfm2['NEW DFU_x'].isnull()),
    (dfm2['NEW DFU_y']), dfm2['NEW DFU_x']
)

df_o=dfm2[['Year', 'Period', 'DistributorCode', 'Changed DFU','OS']]

df_o = df_o.rename({'Changed DFU': 'DFU'}, axis=1)

# Changing datatype of year
df_o['Year'] = df_o['Year'].fillna(0).astype('int64')
df_o['Period'] = df_o['Period'].fillna(0).astype('int64')
df_o['DistributorCode'] = df_o['DistributorCode'].astype(str).str.upper()
df_o['DFU'] = df_o['DFU'].astype(str)

# Group by and sum to remove duplicates
df_o = df_o.groupby(['Year', 'Period', 'DistributorCode', 'DFU'], as_index=False)['OS'].sum()

# Checking if OS file is empty
if df_o.empty:
    print("OS file is empty")
    df_o = pd.read_csv(os.path.join(filepath,os_filename))

    df_o = df_o.rename(
        {'dfucode': 'DFU', 'volume': 'OS', 'year': 'Year', 'period': 'Period', 'distributorcode': 'DistributorCode'},
        axis=1)

    # Changing datatype
    df_o['Year'] = df_o['Year'].fillna(0).astype('int64')
    df_o['Period'] = df_o['Period'].fillna(0).astype('int64')
    df_o['DistributorCode'] = df_o['DistributorCode'].astype(str)
    df_o['DFU'] = df_o['DFU'].astype(str)



# Calculating YearMonth
df_o['YearMonth'] = df_o['Year'].apply(str) + df_o['Period'].apply(str).str.zfill(2)




#####################################
#  Joining [actuals+forecast-->IMS] and OS
#####################################
df = pd.merge(df, df_o[['YearMonth', 'DistributorCode', 'DFU', 'OS']], how='left', on=['YearMonth', 'DistributorCode', 'DFU'])

########################################
#  Reading web arrivals for past months -- This is the Web Confirmation 
########################################

df_arr = pd.read_csv(os.path.join(filepath,web_filename))

## added plant in condition for SKU to DFU conversion
df_arr['dfu']=df_arr['dfu'].fillna(0).astype('int64')

#converting SKU to DFU 
df_arr['dfu'] = df_arr.apply(lambda x: convert_sku_to_dfu(x['plant'], int(x['dfu'])), axis=1)

df_arr = df_arr.groupby(['distributorcode', 'dfu','year','arrperiod'], as_index=False)['arr'].sum()
df_arr = df_arr.rename({'distributorcode': 'DistributorCode','dfu':'DFU' ,'arr': 'ARR', 'year': 'Year', 'arrperiod': 'Period'}, axis=1)


# Changing SKU codes of web arrivals to DFU code -- OLD logic not needed
#df_arr['DFU'] =  df_arr['dfucode'].astype('str').str[:7] 
#df_arr['DFU'] = df_arr['DFU'].str.pad(10, side='right', fillchar='0')

df_arr['Year'] = df_arr['Year'].astype('int64')
df_arr['Period'] = df_arr['Period'].astype(str)
df_arr['DistributorCode'] = df_arr['DistributorCode'].astype(str)
df_arr['DFU'] = df_arr['DFU'].astype(str)

##########################
## Merging Arrivals against the Bill to customer mapping to get the Distributor Name 
##########################
df_cust_map_sub=df_cust_map[['Sold-to party','Distributor']].drop_duplicates()
df_cust_map_sub['Sold-to party']=df_cust_map_sub['Sold-to party'].astype('str')
df_arr=df_arr.merge(df_cust_map_sub,how='left',left_on='DistributorCode',right_on='Sold-to party')
df_arr['Distributor']=df_arr['Distributor'].fillna(0).str.upper()
df_arr=df_arr.drop(['DistributorCode','Sold-to party'], axis=1)
df_arr=df_arr.rename(columns={'Distributor':'DistributorCode'})

# Changing old dfus which may be present in sap to new dfu codes 

##-- Commented as DF DFU file already read above
#df_dfu = pd.read_csv(os.path.join(filepath,dfu_mapping_filename))
## print(df_dfu)
#df_dfu['NEW DFU'] = df_dfu['NEW DFU'].astype(str)
#df_dfu['OLD DFU'] = df_dfu['OLD DFU'].astype(str)
dfm = pd.merge(df_arr, df_dfu[['NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['NEW DFU'] )
# print(dfm)
dfm2 = pd.merge(dfm, df_dfu[['OLD DFU', 'NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['OLD DFU'] )

dfm2['Changed DFU'] = np.where(
    (dfm2['NEW DFU_x'].isnull()),
    (dfm2['NEW DFU_y']), dfm2['NEW DFU_x']
)


df_arr=dfm2[['Year', 'Period', 'DistributorCode', 'Changed DFU','ARR']]
df_arr = df_arr.rename({'Changed DFU': 'DFU'}, axis=1)


# Group by and sum to remove duplicates
df_arr = df_arr.groupby(['Year', 'Period', 'DistributorCode', 'DFU'], as_index=False)['ARR'].sum()

if df_arr.empty:
    print("Arrivals empty")
    df_arr = pd.read_csv(filepath + web_filename)

    df_arr = df_arr.rename({'distributorcode': 'DistributorCode', 'value': 'ARR', 'year': 'Year', 'period': 'Period','dfu':'dfucode'},
                           axis=1)

    # Changing SKU codes of web arrivals to DFU code
    df_arr['DFU'] = df_arr['dfucode'].str[:7]
    df_arr['DFU'] = df_arr['DFU'].str.pad(10, side='right', fillchar='0')

    df_arr['Year'] = df_arr['Year'].astype('int64')
    df_arr['Period'] = df_arr['Period'].astype(str)
    df_arr['DistributorCode'] = df_arr['DistributorCode'].astype(str)
    df_arr['DFU'] = df_arr['DFU'].astype(str)


# Calculating YearMonth column
df_arr['YearMonth'] = df_arr['Year'].apply(str) + (df_arr['Period'].str.replace('P', '')).str.zfill(2)


########################################
# Joining actuals, forecast, os and arrivals (arrivals has past data till now and OS has data till present month)
########################################
df = pd.merge(df, df_arr[['DistributorCode', 'DFU', 'YearMonth', 'ARR']], how='left', on=['DistributorCode', 'DFU', 'YearMonth'])
df.to_csv(os.path.join(filepath,'30_MPW_DebugFileAfterJoinActualForecastOSArrival.csv'))


##############################
# Bringing Adj of past months
##############################
df_adj = pd.read_csv(os.path.join(filepath,adj_filename))

df_adj = df_adj.rename({'year': 'Year', 'period': 'Period', 'distributorcode': 'DistributorCode', 'dfucode': 'DFU', 'volume': 'ADJ'}, axis=1)

# Changing datatype of year
df_adj['Year'] = df_adj['Year'].astype('int64')
df_adj['Period'] = df_adj['Period'].astype(str)
df_adj['DistributorCode'] = df_adj['DistributorCode'].astype(str)
df_adj['DFU'] = df_adj['DFU'].astype(str)

dfm = pd.merge(df_adj, df_dfu[['NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['NEW DFU'] )
# print(dfm)
dfm2 = pd.merge(dfm, df_dfu[['OLD DFU', 'NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['OLD DFU'] )

dfm2['Changed DFU'] = np.where(
    (dfm2['NEW DFU_x'].isnull()),
    (dfm2['NEW DFU_y']), dfm2['NEW DFU_x']
)


df_adj=dfm2[['Year', 'Period', 'DistributorCode', 'Changed DFU','ADJ']]
df_adj = df_adj.rename({'Changed DFU': 'DFU'}, axis=1)
# Group by and sum to remove duplicates
df_adj = df_adj.groupby(['Year', 'Period', 'DistributorCode', 'DFU'], as_index=False)['ADJ'].sum()

if df_adj.empty:
    print('No Adjustment data in file')
    print('ADJ Test')
    df_adj = pd.read_csv(os.path.join(filepath,adj_filename))
    df_adj = df_adj.rename(
        {'year': 'Year', 'period': 'Period', 'distributorcode': 'DistributorCode', 'dfucode': 'DFU', 'volume': 'ADJ'},
        axis=1)
print('Adjustment file processed')
# Calculating YearMonth column
df_adj['YearMonth'] = df_adj['Year'].apply(str) + (df_adj['Period'].str.replace('P', '')).str.zfill(2)


##############################
# Joining actual, forecast, os, arrivals and adjustments (ARR and ADJ has past data and OS has data till present month)
#############################
df = pd.merge(df, df_adj[['DistributorCode', 'DFU', 'YearMonth', 'ADJ']], how='left', on=['YearMonth', 'DistributorCode', 'DFU'])


df.to_csv(os.path.join(filepath,'31_MPW_DebugFileAfterJoinAdjustment.csv'))

# Adding flag indicator for identifying calculated arrivals and presently defaulting it 0
df['flag'] = 0


# Calculating YearMonth for last month and next two months
if int(curr_p) == 1:
    last_month = 12
    last_ym = str(int(curr_yr)-1)+str(last_month).zfill(2)
else:
    last_month = int(curr_p)-1
    last_ym = str(curr_yr) + str(last_month).zfill(2)

# Get data for current month and last month to calculate Adj
df_adj_lm = df[(df['YearMonth'] == curr_yearmonth) | (df['YearMonth'] == last_ym)]
df_adj_lm['Next_month_os'] = df_adj_lm.groupby(['DistributorCode', 'DFU'], as_index=False)['OS'].shift(-1)
df_adj_lst_mnth = df_adj_lm[df_adj_lm['YearMonth'] == last_ym]
df_adj_lst_mnth['ADJ'] = df_adj_lst_mnth['Next_month_os'] - (df_adj_lst_mnth['OS']+df_adj_lst_mnth['ARR']-df_adj_lst_mnth['IMS'])
df_adj_lst_mnth = df_adj_lst_mnth.drop(['Next_month_os'], axis=1)

# Creating dataframe to store ADJ of last month which get stored in table
df_file_for_adj = df_adj_lst_mnth[['YearMonth', 'DistributorCode', 'DFU', 'ADJ']]
df_file_for_adj['Year'] = df_file_for_adj['YearMonth'].str[:4]
df_file_for_adj['Period'] = df_file_for_adj['YearMonth'].str[4:]
df_file_for_adj['Period'] = df_file_for_adj['Period'].astype('int64')
df_file_for_adj['Period'] = 'P' + df_file_for_adj['Period'].astype(str)
df_file_for_adj = df_file_for_adj.rename({'ADJ': 'Value'}, axis=1)
df_file_for_adj = df_file_for_adj[['Year', 'Period', 'DistributorCode', 'DFU', 'Value']]

#####################################################################################################
# Writing ADJ output to csv file
try:
    print("adj out test")
    df_file_for_adj.to_csv(os.path.join(filepath,adj_output), index=False)
except Exception as e:
    print(str(e))
    raise Exception(e)
    exit(-1)
#####################################################################################################

## Droping the last year month rows and appendind with ADJ calculated with last year month
df = df.drop(df.loc[df['YearMonth'] == last_ym].index)
df = df.append(df_adj_lst_mnth, ignore_index=True)[df_adj_lst_mnth.columns.tolist()]
df = df.sort_values(['DistributorCode', 'DFU', 'YearMonth'], ascending=[True, True, True])

# Updating Adj of present and future months as zero
df.loc[df['YearMonth'] >= curr_yearmonth, 'ADJ'] = 0

# Creating dataframe having present month and future month data to calculate future OS and ARR
df1 = df[df['YearMonth'] >= curr_yearmonth]

# Getting SAP arrivals
df_sap = pd.read_csv(os.path.join(filepath,sap_filename))

df_sap=df_sap.drop(['DistributorCode','Sold-to party'], axis=1)
df_sap=df_sap.rename(columns={'Distributor':'DistributorCode'})
df_sap=df_sap[['DistributorCode', 'DFU', 'Year', 'Period', 'SAP_ARR', 'MaxYearMonth']]
df_sap = df_sap.rename({'distributor': 'DistributorCode', 'dfu': 'DFU', 'year': 'Year', 'period': 'Period'}, axis=1)

df_sap = df_sap.groupby(['DistributorCode', 'DFU', 'Year','Period','MaxYearMonth'], as_index=False)['SAP_ARR'].sum()


# Changing datatype of year
df_sap['Year'] = df_sap['Year'].astype('int64')
df_sap['Period'] = df_sap['Period'].astype(str)
df_sap['DistributorCode'] = df_sap['DistributorCode'].astype(str)
df_sap=df_sap[df_sap['DFU'].notna()]
df_sap['DFU'] = df_sap['DFU'].astype('int64').astype('str')

# Calculating YearMonth for sap arrivals
df_sap['YearMonth'] = df_sap['Year'].apply(str) + (df_sap['Period'].str.replace('P', '')).str.zfill(2)
df_sap['MaxYearMonth'] = pd.to_datetime(df_sap['MaxYearMonth'], format='%Y%m').dt.strftime('%Y%m')

# Calculating leftover of previous month
if int(curr_p) == 1:
    prev_yearmonth = str(int(curr_yr)-1)+'12'
else:
    prev_yearmonth = str(curr_yr)+str(int(curr_p)-1).zfill(2)


# Creating dataframe with previous month data for calculating leftover
df2 = df[(df['YearMonth'] == prev_yearmonth)]
##** Commneted to brinh new left over lofic **##
#if one_time_upload == 1:
#    print("There is no data for previous month which means this is first time upload")
#
#    # Checking whether leftover file is present or not
#    if not path.exists(filepath + leftover_filename):
#        raise Exception("Leftover file not present. Quiting further processing...")
#        exit(-1)
#
#    # Reading leftover file - This is one time activity
#    df_leftover = pd.read_csv(filepath+leftover_filename)
#    df_leftover = df_leftover.rename({'ARR': 'Leftover'}, axis=1)
#
#
#    # Changing distributor code and DFU code to string datatype
#    df_leftover['DistributorCode'] = df_leftover['DistributorCode'].astype(str)
#    df_leftover['DFU'] = df_leftover['DFU'].astype(str)
#
#    
#    ## Changing mapping D-code to D-name
#    df_cust_map_sub=df_cust_map[['Sold-to party','Distributor']].drop_duplicates()
#    df_cust_map_sub['Sold-to party']=df_cust_map_sub['Sold-to party'].astype('str')
#
#    df_leftover=df_leftover.merge(df_cust_map_sub,how='left',left_on='DistributorCode',right_on='Sold-to party')
#    
#    df_leftover=df_leftover.drop(['Sold-to party','DistributorCode'],axis=1)
#    df_leftover=df_leftover.rename(columns={'Distributor':'DistributorCode'})
#    df_leftover['DistributorCode']=df_leftover['DistributorCode'].fillna(0).astype('str').str.upper()
#    df_leftover = df_leftover.groupby(['DistributorCode', 'DFU'], as_index=False)['Leftover'].sum()
#    # Assigning this df to df2 which is later joined with current month dataframe to calculate arrivals
#    del df2   # Before this if loop we had assigned last month arrival data to df2, now it is one time upload, so clearing this and setting it to leftover
#    df2 = df_leftover.copy()
#    df2 = df2.fillna(0)
#    
#
#else:
#    # So else part will execute when it is NOT first time upload
#    # Checking whether pipeline report for last month file is present or not as it is very much needed in calculation of leftovers
#    if not path.exists(filepath + last_month_pipe_filename):
#        raise Exception("Last month pipeline report file not present. Quiting further processing...")
#        exit(-1)
#
#    # Read last month pipeline report to get calculated Arrival of last month and subtract from web arrivals confirmed to get leftovers
#    df_lm_cal_arr = pd.read_csv(filepath + last_month_pipe_filename)
#    # Change datatype to str of dcode and sku
#    df_lm_cal_arr['DistributorCode'] = df_lm_cal_arr['DistributorCode'].astype(str)
#    df_lm_cal_arr['DFU'] = df_lm_cal_arr['DFU'].astype(str)
#    df_lm_cal_arr['YearMonth'] = df_lm_cal_arr['YearMonth'].astype(int)
#    prev_yearmonth = int(prev_yearmonth)
#    # Though this file should have data for only last month but still as a check filter data for last month
#    df_lm_cal_arr = df_lm_cal_arr[(df_lm_cal_arr['YearMonth'] == prev_yearmonth)]
#    # print(df_lm_cal_arr)
#    # Rename ARR to CAL_ARR
#    df_lm_cal_arr = df_lm_cal_arr.rename({'ARR': 'CAL_ARR'}, axis=1)
#
#    df2 = pd.merge(df2, df_lm_cal_arr[['DistributorCode', 'DFU', 'YearMonth', 'CAL_ARR']], how='left', on=['YearMonth', 'DistributorCode', 'DFU'])
#    df2['Leftover'] = df2['CAL_ARR'] - df2['ARR']
#    df2 = df2.fillna(0)

### Reding leftover data from web confirmation file uploaded and extracting through Hive

df_leftover = pd.read_csv(os.path.join(filepath,leftover_filename))
df_leftover = df_leftover.rename({'ARR': 'Leftover'}, axis=1)


# Changing distributor code and DFU code to string datatype
df_leftover['DistributorCode'] = df_leftover['DistributorCode'].astype(str)
df_leftover['DFU'] = df_leftover['DFU'].astype(str)

    
## Changing mapping D-code to D-name
df_cust_map_sub=df_cust_map[['Sold-to party','Distributor']].drop_duplicates()
df_cust_map_sub['Sold-to party']=df_cust_map_sub['Sold-to party'].astype('str')
df_leftover=df_leftover.merge(df_cust_map_sub,how='left',left_on='DistributorCode',right_on='Sold-to party')
    
df_leftover=df_leftover.drop(['Sold-to party','DistributorCode'],axis=1)
df_leftover=df_leftover.rename(columns={'Distributor':'DistributorCode'})
df_leftover['DistributorCode']=df_leftover['DistributorCode'].fillna(0).astype('str').str.upper()
df_leftover = df_leftover.groupby(['DistributorCode', 'DFU'], as_index=False)['Leftover'].sum()

if df_leftover.empty:
    print("Leftover file is empty")
    df_leftover = pd.read_csv(os.path.join(filepath,leftover_filename))
    df_leftover=df_leftover.rename(columns={'distributorcode':'DistributorCode','dfu':'DFU','ARR':'Leftover'})
    df_leftover = df_leftover[['DistributorCode', 'DFU', 'Leftover']]

# Adding leftover to current month arrival data

df_curr_arr = df[(df['YearMonth'] == curr_yearmonth)]
df_curr_arr['DistributorCode'] = df_curr_arr['DistributorCode'].astype(str)
df_curr_arr['DFU'] = df_curr_arr['DFU'].astype(str)
df_curr_arr = pd.merge(df_curr_arr, df_leftover[['DistributorCode', 'DFU', 'Leftover']], how='left', on=['DistributorCode', 'DFU'])
df_curr_arr.to_csv(os.path.join(filepath,'32_MPW_DebugFileCurrArrivalLeftover.csv'))
# print(df_curr_arr)
df_curr_arr['ARR'] = df_curr_arr['Leftover']
df_curr_arr['ARR'] = df_curr_arr['ARR'].fillna(0)
df_curr_arr = df_curr_arr.drop(['Leftover'], axis=1)

#print(df_curr_arr.dtypes)
#print(df_sap.dtypes)

df_curr_arr = pd.merge(df_curr_arr, df_sap[['DistributorCode', 'DFU', 'YearMonth', 'SAP_ARR']], how='left', on=['YearMonth', 'DistributorCode', 'DFU'])

## Total Current month Arrivals Leftovers + Actual Shipment
df_curr_arr['ARR'] = df_curr_arr['ARR'].fillna(0) + df_curr_arr['SAP_ARR'].fillna(0)
df_curr_arr = df_curr_arr.drop(['SAP_ARR'], axis=1)


# Delete current month data and append back with actual current month data of arrivals
df1 = df1.drop(df1.loc[df1['YearMonth'] == curr_yearmonth].index)
df1 = df1.append(df_curr_arr, ignore_index=True, sort=True)[df_curr_arr.columns.tolist()]
df1 = df1.sort_values(['DistributorCode', 'DFU', 'YearMonth'], ascending=[True, True, True])
df1 = pd.merge(df1, df_sap[['DistributorCode', 'DFU', 'YearMonth', 'SAP_ARR']], how='left', on=['YearMonth', 'DistributorCode', 'DFU'])

###

# Reading dfu mapping file to get country, category,brand, conversion factor by joining on new dfu
df_dfu = pd.read_csv(os.path.join(filepath,dfu_mapping_filename))
df_dfu = df_dfu.rename({'NEW DFU': 'DFU', 'NEW DESC': 'DFUDescription', 'Conversion_factor_case_to_tons': 'conversion_factor'}, axis=1)

# Joining mapping file with data based on New DFU code and getting Category, Brand and Conversion factor
df_dfu['DFU'] = df_dfu['DFU'].astype(str)
df1 = pd.merge(df1, df_dfu[['DFU','Category']].drop_duplicates(), how='left', on=['DFU'])



###

# Add column with stock cover days for each distributor ## Readong SCD mapping File
df_scd = pd.read_csv(os.path.join(filepath,stock_cover_days_filename))
df_scd=df_scd[['Distributor','Category', 'Days']]
df_scd=df_scd.rename(columns={'Distributor':'DistributorCode'})
# Changing Distributor code and DFU code to string datatype
df_scd['DistributorCode'] = df_scd['DistributorCode'].astype(str).str.upper()
df1['DistributorCode'] = df1['DistributorCode'].astype(str)

df1 = pd.merge(df1, df_scd[['DistributorCode','Category', 'Days']], how='left', on=['DistributorCode','Category'])


# Replace NaN with zero for calculation purpose
df1['IMS'] = df1['IMS'].fillna(0)
df1['OS'] = df1['OS'].fillna(0)
df1['ARR'] = df1['ARR'].fillna(0)


# Joining with SAP arrivalsMaxYearMonth
df_sap_f = df_sap[['DistributorCode', 'DFU', 'YearMonth', 'MaxYearMonth', 'SAP_ARR']]
df_sap_f = df_sap_f.rename({'SAP_ARR': 'SAP'}, axis=1)


# Below logic to eliminate calculation of suggested order when sap for future months is available
# Converting YearMonth column to date format and storing in different column 'YM' to maintain datatype
df_sap_f['YM'] = df_sap_f['YearMonth']
df_sap_f['YM'] = pd.to_datetime(df_sap_f['YM'], format='%Y%m').dt.strftime('%Y%m')
# Store another column in datetime format to be used in future for comparision
df_sap_f['dt_YearMonth'] = df_sap_f['YM']
df1 = pd.merge(df1, df_sap_f[['DistributorCode', 'DFU', 'YearMonth', 'SAP', 'dt_YearMonth', 'MaxYearMonth']], how='left', on=['YearMonth', 'DistributorCode', 'DFU'])

## Calculation Max of year month of SAP data at Distributor and Dist DFU level
df1['MaxYearMonth'] = df1['MaxYearMonth'].fillna(0).astype('int64')
df1['MaxYearMonth_Dis_DFU'] = df1.groupby(['DistributorCode', 'DFU'], as_index=False).MaxYearMonth.transform(max)
df1['MaxYearMonth_Dis'] = df1.groupby(['DistributorCode'], as_index=False).MaxYearMonth.transform(max)

## Condition to get Max of period for each row even when no SAP order is present
df1['MaxYearMonth_final'] = np.where(
    (df1['MaxYearMonth'] == 0),
    (
        np.where(
            df1['MaxYearMonth_Dis_DFU'] == 0, df1['MaxYearMonth_Dis'], df1['MaxYearMonth_Dis_DFU']
        )
    ),
    df1['MaxYearMonth']
)

df1.rename(columns={'MaxYearMonth': 'MaxYearMonth_old', 'MaxYearMonth_final': 'MaxYearMonth'}, inplace=True)

#df1.to_csv('/home/inca1n04/Project/KAMEA/Output/max_date_check.csv')
#df1.to_csv('max_date_check.csv')



# Sorting on the basis of distributor code, dfu code and yearmonth column
df1 = df1.sort_values(['DistributorCode', 'DFU', 'YearMonth'], ascending=[True, True, True])
df1 = df1.reset_index()

# print(df1)

# Adding YearMonth column in date format to comapre with MaxYearMonth of a distributor and incorporate suggested order logic
df1['Date_YearMonth'] = pd.to_datetime(df1['YearMonth'], format='%Y%m').dt.strftime('%Y%m')

df1['Date_YearMonth'] = df1['Date_YearMonth'].astype('int64')
df1['MaxYearMonth'] = df1['MaxYearMonth'].astype('int64')
#df1.to_csv("check OS SO.csv")
# Calculating future OS and arrivals
for i in range(0, len(df1)-1):
    # Assigning present sku and distibutor code in flag and checking if next sku or ditributor code is same.
    # This is basically to group as per sku and distributor code and calculate data for future months(sorting yearmonth is already done above)
    flag_DistributorCode = df1["DistributorCode"][i]
    flag_DFU = df1["DFU"][i]
    flag_MaxYearMonth = df1["MaxYearMonth"][i]
    if isinstance(flag_MaxYearMonth, str) == False:

        if math.isnan(flag_MaxYearMonth):
            str_prev_YearMonth = prev_yearmonth+'01'
            flag_MaxYearMonth = datetime.datetime.strptime(str_prev_YearMonth, '%Y%m%d').strftime("%Y%m%d")

    try:
        if (df1["DistributorCode"][i+1] == flag_DistributorCode) and (df1["DFU"][i+1] == flag_DFU):

            df1["OS"][i + 1] = df1["OS"][i] + max(df1["ARR"][i],0) - df1["IMS"][i] + df1["ADJ"][i]

            if (df1["DistributorCode"][i+2] == flag_DistributorCode) and (df1["DistributorCode"][i+3] == flag_DistributorCode) and (df1["DFU"][i+2] == flag_DFU) and (df1["DFU"][i+3] == flag_DFU):

                # Checking if SAP ARR is null, then calculate else assign SAP value
                if df1["Date_YearMonth"][i+1] > flag_MaxYearMonth:
                    df1['flag'][i + 1] = 1
                    df1["ARR"][i + 1] = (((df1["IMS"][i+2] + df1["IMS"][i+3])/61)*df1["Days"][i+1]) - (df1["OS"][i+1] - df1["IMS"][i+1])

                elif math.isnan(df1["SAP"][i+1]) and df1["Date_YearMonth"][i+1] <= flag_MaxYearMonth:
                    df1['flag'][i + 1] = 0
                    df1["ARR"][i+1] = 0.0
                    df1['MaxYearMonth'][i+1] = flag_MaxYearMonth

                else:
                    df1["ARR"][i+1] = df1["SAP"][i+1]


            # Checking next second month distributor code and SKU is same

            elif df1["DistributorCode"][i+2] == flag_DistributorCode and df1["DFU"][i+2] == flag_DFU:

                if math.isnan(df1["SAP"][i+1]):

                    df1['flag'][i + 1] = 1

                    df1["ARR"][i + 1] = (((df1["IMS"][i+2] + 0.0)/61)*df1["Days"][i+1]) - (df1["OS"][i+1] - df1["IMS"][i+1])

                else:
                    df1["ARR"][i+1] = df1["SAP"][i+1]


            else:
                if math.isnan(df1["SAP"][i+1]):

                    df1['flag'][i + 1] = 1

                    df1["ARR"][i + 1] = (((0.0 + 0.0)/61)*df1["Days"][i+1]) - (df1["OS"][i+1] - df1["IMS"][i+1])

                else:
                    df1["ARR"][i+1] = df1["SAP"][i+1]


    except:

        # Since we are going index wise so checking if index has reached out of range
        if (i+2) >= len(df)-1:

            df1['flag'][i + 1] = 1
            df1["ARR"][i + 1] = (((0.0 + 0.0)/61)*df1["Days"][i+1]) - (df1["OS"][i + 1] - df1["IMS"][i + 1])

        elif (i+3) >= len(df)-1:

            df1['flag'][i + 1] = 1
            df1["ARR"][i + 1] = (((df1["IMS"][i + 2] + 0.0)/61)*df1["Days"][i+1]) - (df1["OS"][i + 1] - df1["IMS"][i + 1])



# Appending these calculated values back in df
df1 = df1.drop(['SAP_ARR'], axis=1)
df = pd.merge(df, df_dfu[['DFU','Category']].drop_duplicates(), how='left', on=['DFU'])
df = df.drop(df.loc[df['YearMonth'] >= curr_yearmonth].index)
df = df.append(df1, ignore_index=True, sort=True)[df1.columns.tolist()]
df = df.sort_values(['DistributorCode', 'DFU', 'YearMonth'], ascending=[True, True, True])

# Deleting days as new dataframe doesn't have days for past months
df = df.drop(['Days'], axis=1)




# Add column with stock cover days for each distributor
df_sc = pd.read_csv(os.path.join(filepath,stock_cover_days_filename))
df_sc=df_sc[['Distributor','Category', 'Days']]
df_sc=df_sc.rename(columns={'Distributor':'DistributorCode'})
df_sc['DistributorCode'] = df_sc['DistributorCode'].astype(str).str.upper()
df['DistributorCode'] = df['DistributorCode'].astype(str).str.upper()
df = pd.merge(df, df_sc[['DistributorCode', 'Days','Category']], how='left', on=['DistributorCode','Category'])
df = df.reset_index()

# Calculating WOC for all months( past, present and future)
df['WOC'] = 0.0
for j in range(0, len(df)):
    flag_DistributorCode = df['DistributorCode'][j]
    flag_DFU = df['DFU'][j]
    try:
        if df['DistributorCode'][j+1] == flag_DistributorCode and df['DFU'][j+1] == flag_DFU:
            df['WOC'][j] = df['OS'][j] / (df['IMS'][j] + df['IMS'][j + 1]) * 61  #df["Days"][j]

        else:
            df['WOC'][j] = df['OS'][j] / (0.0 + 0.0) * 61 #df["Days"][j]

    except:
        if j == len(df)-1:
            df['WOC'][j] = df['OS'][j] / (0.0+0.0)* 61  #df["Days"][j]
        elif (j+1) >= len(df):
            df['WOC'][j] = df['OS'][j] / (0.0 + 0.0) * 61 # df["Days"][j]


# Reformating columns in tableau format
# df = df[['Year', 'Quarter', 'Month', 'Period', 'YearMonth', 'Cycle', 'DistributorCode', 'DFU', 'DFUDescription', 'Brand', 'Version', 'OS', 'ARR', 'IMS', 'WOC', 'ADJ', 'flag']]
df = df[['Year', 'Quarter', 'Month', 'Period', 'YearMonth', 'Cycle', 'DistributorCode', 'DFU', 'DFUDescription', 'Brand', 'Version', 'OS', 'ARR', 'IMS', 'WOC', 'ADJ', 'flag', 'Days', 'SAP', 'Date_YearMonth', 'MaxYearMonth']]
df = df.rename({'DFUDescription': 'DFUDescription_old'}, axis=1)

# Reading dfu mapping file to get country, category,brand, conversion factor by joining on new dfu
df_dfu = pd.read_csv(os.path.join(filepath,dfu_mapping_filename))
df_dfu = df_dfu.rename({'NEW DFU': 'DFU', 'NEW DESC': 'DFUDescription', 'Conversion_factor_case_to_tons': 'conversion_factor'}, axis=1)


# Renaming brand as brand_frm_ac in df
df = df.rename({'Brand': 'Brand_frm_ac'}, axis=1)


# Joining mapping file with data based on New DFU code and getting Category, Brand and Conversion factor
df_dfu['DFU'] = df_dfu['DFU'].astype(str)
#df.to_csv('df_before.csv')
df = pd.merge(df, df_dfu[['DFU', 'DFUDescription', 'Brand', 'Category', 'conversion_factor']].drop_duplicates(), how='left', on=['DFU'])
#df.to_csv('df_after.csv')

# Reading dfu mapping file to get country, category,brand, conversion factor, old and new dfu
#df_cust_map = pd.read_csv(filepath+customer_country_filename) ## Created at the top --Neeraj
df_cust_map = df_cust_map.rename({'Cluster': 'country_group', 'Market': 'Country', 'Sold-to party': 'DistributorCode', 'Distributor': 'DistributorName'}, axis=1)
df_cust_map['DistributorCode'] = df_cust_map['DistributorCode'].astype(str)
df_cust_map['DistributorName'] = df_cust_map['DistributorName'].astype(str).str.upper()
df = pd.merge(df, df_cust_map[['DistributorName', 'country_group', 'Country']].drop_duplicates(), how='left', left_on=['DistributorCode'],right_on=['DistributorName'])


# Reformat output in tableau format
df = df[['Year', 'Quarter', 'Month', 'Period', 'YearMonth', 'Cycle', 'DistributorCode', 'DistributorName', 'Country', 'DFU', 'DFUDescription', 'Brand', 'Category', 'Version', 'country_group', 'conversion_factor', 'OS', 'ARR', 'IMS', 'WOC', 'ADJ', 'flag', 'Days', 'SAP', 'Date_YearMonth', 'MaxYearMonth']]

#df.to_csv("check negative op.csv")
df = df.replace([np.inf, -np.inf], 0)

##############################
# Reformat for python report table
##############################
df_ans1 = df[['Year', 'Quarter', 'Month', 'Period', 'YearMonth', 'Cycle', 'DistributorCode', 'DistributorName', 'Country', 'DFU', 'DFUDescription', 'Brand', 'Category', 'Version', 'country_group', 'conversion_factor', 'OS', 'ARR', 'IMS', 'WOC', 'ADJ', 'flag','Days']]
df_ans1['load_date']=dt.now().strftime('%Y-%m-%d %H:%M:%S')
print(df_ans1)

# Writing data to csv file to be used in tableau hive table
try:
    df_ans1.to_csv(filepath+python_csv_output, index=False)
except Exception as e:
    raise Exception(e)
    exit(-1)



##############################
# Reformat for Excel report
##############################
df_ans2 = df[['Year', 'Quarter', 'Month', 'Period', 'YearMonth', 'Cycle', 'DistributorCode', 'DistributorName', 'Country', 'DFU', 'DFUDescription', 'Brand', 'Category', 'Version', 'country_group', 'conversion_factor', 'OS', 'ARR', 'IMS', 'WOC', 'ADJ', 'flag', 'Days']]

# Writing data to excel file
try:
    df_ans2.to_excel(filepath+python_excel_output, index=False)
except Exception as e:
    raise Exception(e)
    exit(-1)
