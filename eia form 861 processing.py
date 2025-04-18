#%%
import pandas as pd
import numpy as np

#%%
# Define column names
column_names = [
    'Year', 'Utility_ID', 'Utility_Name', 'Utility_Part', 'Utility_Service_Type', 
    'Utility_Data_Type', 'Utility_State', 'Utility_Ownership', 'Utility_BA_Code', 
    'Res_Rev', 'Res_Sales_MWH', 'Res_Cust', 'Com_Rev', 'Com_Sales_MWH', 'Com_Cust', 
    'Ind_Rev', 'Ind_Sales_MWH', 'Ind_Cust', 'Transport_Rev', 'Transport_Sales_MWH', 
    'Transport_Cust', 'Total_Rev', 'Total_Sales_MWH', 'Total_Cust'
]

rootpath = "C:\\Users\\FredSchaefer\\Downloads\\"
sheet_name = "States"

yr_list = [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
util_id = 15248

# Read the Excel file
df_collect = []

for y in yr_list :
    df = pd.read_excel(rootpath+"f861"+str(y)+"\\Sales_Ult_Cust_"+str(y)+".xlsx", sheet_name=sheet_name, skiprows=3, names=column_names)
    float_columns = ['Utility_ID','Res_Rev', 'Res_Sales_MWH', 'Res_Cust', 'Com_Rev', 'Com_Sales_MWH', 'Com_Cust', 'Ind_Rev', 'Ind_Sales_MWH', 'Ind_Cust', 'Transport_Rev', 'Transport_Sales_MWH', 
        'Transport_Cust', 'Total_Rev', 'Total_Sales_MWH', 'Total_Cust']
    df[float_columns] = df[float_columns].apply(pd.to_numeric, errors='coerce')
    df_collect.append(df)

# Display the first few rows
df_eia = pd.concat(df_collect)
#%%
df_pge = df_eia[df_eia['Utility_ID']==util_id]
df_pge.info()
# %%
df_pge2 = df_pge.groupby(['Year','Utility_ID','Utility_Name']).agg({'Res_Rev':'sum', 'Res_Sales_MWH':'sum', 'Res_Cust':'sum', 'Com_Rev':'sum', 'Com_Sales_MWH':'sum', 'Com_Cust':'sum', 
    'Ind_Cust':'sum','Ind_Rev':'sum', 'Ind_Sales_MWH':'sum'}).reset_index()
df_pge2.to_csv('C:\\Users\\FredSchaefer\\OneDrive - Resource-Innovations\\Desktop\\PGE\\form_861_out.csv', index=False)
# %%
# %%
