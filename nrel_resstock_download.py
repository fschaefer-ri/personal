#%% import packages

import pandas as pd
import numpy as np

# datapath = "C:\\Users\\FredSchaefer\\OneDrive - CADEO GROUP, LLC\\Desktop\\PGE stuff\\BE update"

#%%
url = 'https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_tmy3_release_2/metadata_and_annual_results/by_state/state=OR/parquet/OR_baseline_metadata_and_annual_results.parquet'
dfr = pd.read_parquet(url)
dfr['heating_fuel'] = np.where(dfr['in.heating_fuel']=='Electricity','Elec','Non-Elec')
dfr['wh_fuel'] = np.where(dfr['in.water_heater_fuel']=='Electricity','Elec','Non-Elec')
dfr['has_cooling'] = np.where(dfr['in.hvac_cooling_type']=='None','N','Y')
dfr['has_ducts'] = np.where(dfr['in.hvac_heating_type']=='Ducted Heating','Y','N')
dfr['cooking_fuel'] = np.where(dfr['in.cooking_range'].isin(['Electric Resistance','Electric Induction']),'Elec','Non-Elec')

dict_bldg_type={'Single-Family Detached':'SF','Multi-Family with 2 - 4 Units':'SF','Multi-Family with 5+ Units':'MF','Single-Family Attached':'SF','Mobile Home':'MH'}
dfr['bldg_type']=dfr['in.geometry_building_type_recs'].map(dict_bldg_type)
dfr = dfr.reset_index()

# filter by non-electric heating fuel, weather station, and rank (to subset down to manageble number)
dfr = dfr[(dfr['in.weather_file_city'].isin(['Portland International Ap','Portland Hillsboro']))&(dfr['heating_fuel']=='Non-Elec')]
dfr = dfr[['bldg_id','bldg_type','heating_fuel','wh_fuel','has_ducts','cooking_fuel','has_cooling']]

#%%
# Set up some stratifed sampling - focus on MF and SF, non-ducted 
sf_df1 = dfr[(dfr['bldg_type'] == 'SF')&(dfr['has_ducts'] == 'N')]
sf_df2 = dfr[(dfr['bldg_type'] == 'SF')&(dfr['has_ducts'] == 'Y')]
mf_df = dfr[dfr['bldg_type'] == 'MF']

# Then, we sample the desired number of rows from each
sf1_sample = sf_df1.sample(n=10, random_state=421)  # Random state for reproducibility
sf2_sample = sf_df2.sample(n=10, random_state=421)  # Random state for reproducibility
mf_sample = mf_df.sample(n=10, random_state=421)

# Combine the sampled dataframes
dfr2 = pd.concat([sf1_sample, sf2_sample, mf_sample])
dfr2
#%% for building electrification focus, focus on specific upgrade packages and on buildings that upgrade from non-electric fuel.
# see https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_tmy3_release_2/resstock_documentation_2024_release_2.pdf 
# 0 = baseline
# 2 = High efficiency cold-climate heat pump with elec backup
# 4 = ENERGY STAR heat pump with existing system as backup
# 14 = ENERGY STAR heat pump with existing system as backup + Light Touch Envelope + Full Appliance Electrification with Efficiency
bldgs=list(dfr2['bldg_id'].values)
upgrades=[0,2,4,14]

# for each building in sample, download end-use interval data for each upgrade
df_list = []
for b in bldgs:
    for u in upgrades :
        url = 'https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_tmy3_release_2/timeseries_individual_buildings/by_state/upgrade='+str(u)+'/state=OR/'+str(b)+'-'+str(u)+'.parquet'
        try: 
            tmp = pd.read_parquet(url)
            # tmp = tmp[['timestamp','out.electricity.cooling_fans_pumps.energy_consumption','out.electricity.cooling_fans_pumps.energy_consumption_intensity','out.electricity.cooling.energy_consumption','out.electricity.cooling.energy_consumption_intensity','out.electricity.heating_fans_pumps.energy_consumption','out.electricity.heating_fans_pumps.energy_consumption_intensity','out.electricity.heating_hp_bkup.energy_consumption','out.electricity.heating_hp_bkup.energy_consumption_intensity','out.electricity.heating.energy_consumption','out.electricity.heating.energy_consumption_intensity','out.electricity.pv.energy_consumption','out.electricity.pv.energy_consumption_intensity','out.electricity.net.energy_consumption','out.electricity.net.energy_consumption_intensity','out.electricity.total.energy_consumption','out.electricity.total.energy_consumption_intensity']]
            tmp['upgrade']=u
            # calculate end-use EUI
            tmp['eui_heat']=tmp['out.electricity.heating.energy_consumption_intensity']+tmp['out.electricity.heating_fans_pumps.energy_consumption_intensity']+tmp['out.electricity.heating_hp_bkup.energy_consumption_intensity']
            tmp['eui_cool']=tmp['out.electricity.cooling_fans_pumps.energy_consumption_intensity']+tmp['out.electricity.cooling.energy_consumption_intensity']
            tmp['eui_wh']=tmp['out.electricity.hot_water.energy_consumption_intensity']
            tmp['eui_cook']=tmp['out.electricity.range_oven.energy_consumption_intensity']
            tmp = tmp.reset_index()
            # filter the columns
            tmp = tmp[['bldg_id','timestamp','upgrade','eui_heat','eui_cool','eui_wh','eui_cook']]

            df_list.append(tmp)
            print(f"****success:  bldg {b} and upgrade {u}")
        except:
            print(f"read failed: bldg {b} and upgrade {u}")

result_df = pd.concat(df_list)

#%% Merge metadata with timeseries
result_df['timestamp'] = result_df['timestamp'] - pd.Timedelta(hours=1)
result_df2 = pd.merge(left=dfr2, right=result_df, on='bldg_id', how='left')

# write to CSV to take it the rest of the way in Excel
# result_df2.to_csv(datapath+os.sep+"resstock_timeseries_avg.csv", index=False)



#%%
aa = result_df2[result_df2['upgrade'].isin([0,2,4])].groupby(['bldg_type','has_ducts','has_cooling','upgrade','timestamp']).agg({'eui_heat':'mean','eui_cool':'mean','bldg_id':'nunique'}).reset_index()
aa.to_csv(datapath+os.sep+"resstock_timeseries_avg_hvac.csv", index=False)

bb = result_df2[(result_df2['wh_fuel']=='Non-Elec')&(result_df2['upgrade'].isin([0,14]))].groupby(['bldg_type','upgrade','timestamp']).agg({'eui_wh':'mean','bldg_id':'nunique'}).reset_index()
bb.to_csv(datapath+os.sep+"resstock_timeseries_avg_wh.csv", index=False)

cc = result_df2[(result_df2['cooking_fuel']=='Non-Elec')&(result_df2['upgrade'].isin([0,14]))].groupby(['bldg_type','upgrade','timestamp']).agg({'eui_cook':'mean','bldg_id':'nunique'}).reset_index()
cc.to_csv(datapath+os.sep+"resstock_timeseries_avg_cook.csv", index=False)
# %%
