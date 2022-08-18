# -*- coding: utf-8 -*-
"""
Date       : 22/02/2021
Author     : Vibha Goswami, Ananda and Neeraj
Description: This script aggreagtes leftover from web confirmation and converts SKUs to DFU.
Project	   : MENAT Pathways - Phase 1
Region     : AMEA
"""

import pandas as pd
import numpy as np
import os

from dotenv import load_dotenv
load_dotenv()
print(os.environ)

path = os.environ.get('filepath')
filename = os.environ.get('leftover_filename')
output_filename = os.environ.get('leftover_output')
dfu_mapping_filename = os.environ.get('dfu_mapping_filename')

## Reading leftover file
df = pd.read_csv(os.path.join(path,filename))
df=df.rename(columns={'distributorcode':'DistributorCode','dfu':'DFU','arr':'ARR'})

## Function to convert SKU to DFUs
def convert_sku_to_dfu(plant, DFU):   
    #print(region)
    #if (system == 'EAP') & ((region).upper()!='SOUTH AFRICA'):
    if (plant not in [5919,5920,6900,6902,6903,344,370,339]):
        DFU = str(DFU)[:-3]+'000'
        return DFU
    else:
        return str(DFU)

df['DFU'] = df['DFU'].fillna(0).astype('int64').astype(str)

df['DistributorCode'] = df['DistributorCode'].astype(str)
df['DFU'] = df.apply(lambda x: convert_sku_to_dfu(x['plant'], x['DFU']), axis=1)
#df['DFU'] = df['DFU'].str[:7]
#df['DFU'] = df['DFU'].str.pad(10, side='right', fillchar='0')

# Changing old dfus which may be present in sap to new dfu codes
df_dfu = pd.read_csv(path+dfu_mapping_filename)

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


df = dfm2[['DistributorCode', 'Changed DFU', 'ARR']]
df = df.rename({'Changed DFU': 'DFU'}, axis=1)
df = df.groupby(['DistributorCode', 'DFU'], as_index=False)['ARR'].sum()
if df.empty:
    print("Leftover file is empty")
    df = pd.read_csv(os.path.join(path,filename))
    df=df.rename(columns={'distributorcode':'DistributorCode','dfu':'DFU','arr':'ARR'})
    df = df[['DistributorCode', 'DFU', 'ARR']]

    
df.to_csv(os.path.join(path,output_filename), index=False)
print(df)
