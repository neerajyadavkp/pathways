# -*- coding: utf-8 -*-
"""
Date       : 22/02/2021
Author     : Vibha Goswami, Ananda and Neeraj
Description: This script combines SAP Invoices and Orders to create SAP Arrivals data at a aggregated level.
Project    : MENAT Pathways - Phase 1
Region     : AMEA
"""

import pandas as pd
import numpy as np
import glob
import os

from dotenv import load_dotenv
load_dotenv()
print(os.environ)


# Remove this code, this is just to view data for testing purpose in pycharm
# pd.set_option('display.width', 300)
# pd.set_option('display.max_columns', 20)

print("starting executing SP files")
filepath = os.environ.get('filepath')
#filepath = r'C:\Users\INKN1Y01\Project\menat_ksop_drm\input3\\'
# filepath = 'C:/Users/M1044306/Documents/Menat Ksop Data/Feb Menat UAT/'
all_files = glob.glob(filepath + "*SAP_Arrivals*.csv")
dfu_mapping_filename = os.environ.get('dfu_mapping_filename')
sap_output_filename = os.environ.get('sap_filename')
customer_country_filename = os.environ.get('customer_country_filename')

if not os.path.exists(os.path.join(filepath,dfu_mapping_filename)):
    raise Exception("DFU mapping file not present. Quiting further processing...")
    exit(-1)
print("starting executing SP files test 2")

# Reading all files in the path folder with wildcard as sap and storing in df
df = []

for filename in all_files:
    try:
        f = pd.read_csv(filename)
        df.append(f)
    except Exception as e:
        print(str(e))
        raise Exception(e)
        exit(-1)

frame = pd.concat(df, axis=0, ignore_index=True)
df = frame[['soldto', 'dfu', 'plantcode', 'transittime', 'cases', 'year', 'arrperiod','billto']]
df = df.rename({'soldto': 'distributor_sap', 'dfu': 'dfu', 'plantcode': 'plantcode', 'year': 'Year', 'arrperiod': 'Period', 'cases': 'Value'}, axis=1)


# Filter rows for null year and period
df = df[df['Year'].notnull()]
df = df[df['Period'].notnull()]


# Changing datatype of Year to int
df['Year'] = df['Year'].astype('int64')

df = df[['distributor_sap', 'dfu', 'plantcode', 'transittime', 'Year', 'Period', 'Value','billto']]
df['distributor_sap'] = df['distributor_sap'].astype(str)

# Filtering records where sap arrival value is positive
# df = df[df['Value'] >= 0]

# Grouping by distributor code, dfu code, plant code, transittime, year, period and agrregating sum
df = df.groupby(['distributor_sap', 'dfu', 'plantcode', 'transittime', 'Year', 'Period','billto'], as_index=False)['Value'].sum()
df['Period'] = 'P' + df['Period'].astype(int).astype(str)
# print(df)
# df.to_csv(path+sap_intermediate_file, index=False)


df['YM'] = df['Year'].apply(str) + (df['Period'].str.replace('P', '')).str.zfill(2)
df = df.rename({'distributor_sap': 'DistributorCode', 'dfu': 'DFU', 'Value': 'SAP_ARR'}, axis=1)
df['DistributorCode'] = df['DistributorCode'].astype(str)
df['billto'] = df['billto'].astype(str)
df['DFU'] = df['DFU'].astype(str)

df['YearMonth'] = pd.to_datetime(df['YM'], format='%Y%m').dt.strftime('%Y%m')

####**** Code added to map the Billto to Distributor Name  ***################

df_stp=pd.read_csv(os.path.join(filepath,customer_country_filename))
df_stp['Sold-to party']=df_stp['Sold-to party'].astype('str')


df=df.merge(df_stp[['Sold-to party','Distributor']].drop_duplicates(),how='left',left_on='billto',right_on='Sold-to party')
df['Distributor']=df['Distributor'].fillna(0).str.upper()

## Implemented logic to get the Max of Period at Distributor and Transittime level, this is needed for creating suggested order caluculation in Integrated Pipeline code
df1 = df.groupby(['Distributor', 'transittime'], as_index=False)['YearMonth'].max()
df1 = df1.rename({'YearMonth': 'MaxYearMonth'}, axis=1)


df = pd.merge(df, df1[['Distributor', 'transittime', 'MaxYearMonth']].drop_duplicates(), how='left', on=['Distributor', 'transittime'])

# df['distributor'] = df['DistributorCode'].str[:5]
# df['distributor'] = df['distributor'].str.pad(7, side='right', fillchar='0')
df2 = df.groupby(['DistributorCode', 'DFU', 'Year', 'Period', 'MaxYearMonth','Sold-to party','Distributor','billto'], as_index=False)['SAP_ARR'].sum()
df2 = df2.rename({'MaxYearMonth': 'MaxYM'}, axis=1)

df2 = df2.groupby(['Distributor', 'DFU'], as_index=False)['MaxYM'].max()

df_final = df.groupby(['DistributorCode', 'DFU', 'YearMonth','Sold-to party','Distributor','billto'], as_index=False)['SAP_ARR'].sum()

df = pd.merge(df_final, df2[['Distributor', 'DFU', 'MaxYM']], how='left', on=['Distributor', 'DFU'])
df['Year'] = df['YearMonth'].str[:4]
df['Period'] = 'P'+df['YearMonth'].str[4:].astype('int').astype(str)
df = df.rename({'MaxYM': 'MaxYearMonth'}, axis=1)
df = df[['DistributorCode', 'DFU', 'Year', 'Period', 'SAP_ARR', 'MaxYearMonth','Sold-to party','Distributor']]


# Changing old dfus which may be present in sap to new dfu codes
df_dfu = pd.read_csv(os.path.join(filepath,dfu_mapping_filename))
df_dfu['NEW DFU'] = df_dfu['NEW DFU'].astype(str)
df_dfu['OLD DFU'] = df_dfu['OLD DFU'].astype(str)
# print(df_dfu)
dfm = pd.merge(df, df_dfu[['NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['NEW DFU'] )
# print(dfm)
dfm2 = pd.merge(dfm, df_dfu[['OLD DFU', 'NEW DFU']].drop_duplicates(), how='left', left_on=['DFU'], right_on=['OLD DFU'] )

dfm2['Changed DFU'] = np.where(
    (dfm2['NEW DFU_x'].isnull()),
    (dfm2['NEW DFU_y']), dfm2['NEW DFU_x']
)
df = dfm2[['DistributorCode', 'Changed DFU', 'Year', 'Period', 'SAP_ARR', 'MaxYearMonth','Sold-to party','Distributor']]
df = df.rename({'Changed DFU': 'DFU'}, axis=1)

print(df)
dfm2=dfm2[dfm2['DFU']!=" "]
df_null = dfm2[dfm2['Changed DFU'].isnull()]
#if not df_null.empty:
#    df_null.to_csv(filepath + 'sap_null_dfus.csv', index=False)
#    raise Exception('Cannot find NEW or OLD DFU in mapping file for some DFUs of SAP')
try:
    print(filepath+sap_output_filename)
    df.to_csv(os.path.join(filepath,sap_output_filename), index=False)
    print("SAP File written")
except Exception as e:
    print(str(e))
    raise Exception(e)
    exit(-1)
