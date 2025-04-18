#%%
import requests
import json
import pandas as pd
from io import StringIO

#%% constants
api_token = 'OK43hWQwb7iVwKafCfhM9LUjS2COSDPmJYEbYacn'
base_url = 'https://developer.nrel.gov/api/alt-fuel-stations/v1.json?'
eid_id_list = [15248]  # this is a comma separated list of EIA IDs (i.e. Utility) to filter to

#%% 
# This URL has ZIP codes in each investor-owned utility service territory as of 2023
# See: https://catalog.data.gov/dataset/?publisher=National+Renewable+Energy+Laboratory+%28NREL%29
zip_url = "https://data.openei.org/files/6225/iou_zipcodes_2023.csv"

# Download the CSV file
response = requests.get(zip_url)
if response.status_code == 200:
    csv_data = StringIO(response.text)
    # Load the CSV file into a DataFrame
    dfz = pd.read_csv(csv_data)
    print("Data loaded successfully!")
else:
    print(f"Failed to download IOU ZIP codes. Status code: {response.status_code}")
    exit()

#%% convert the zip codes in the service territory of the selected utlities to a comma separated string
zip_list = ', '.join(dfz[dfz['eiaid'].isin(eid_id_list)]['zip'].astype(str).tolist())
zip_list

# %% pass comma separated string to NREL AFDC API, convert the returned JSON to a dataframe
afdc_url = base_url + 'zip=' + zip_list + '&fuel_type=ELEC&limit=all&api_key=' + api_token
resp = requests.get(afdc_url)
fuel_station_data = resp.json().get('fuel_stations', [])
df = pd.DataFrame(fuel_station_data)[['station_name', 'street_address', 'city', 'state', 'zip','owner_type_code', 'access_code', 'id', 'latitude', 'longitude',
    'geocode_status', 'ev_connector_types', 'ev_dc_fast_num','ev_level2_evse_num', 'ev_level1_evse_num', 'facility_type']]

#%% some data cleanup - convert counts to numeric, convert NaN and 'None' to zeros
df['ev_dc_fast_num']=df['ev_dc_fast_num'].fillna(0)
df['ev_level1_evse_num']=df['ev_level1_evse_num'].fillna(0)
df['ev_level2_evse_num'] = df['ev_level2_evse_num'].fillna(0)
df.reset_index(inplace=True,drop=True)
df