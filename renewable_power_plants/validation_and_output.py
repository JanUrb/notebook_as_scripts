## remove markdown cells

# coding: utf-8

# In[3]:

# importing all necessary Python libraries for this Script
#%matplotlib inline

import json
import yaml  
import posixpath
import os
import numpy as np
import pandas as pd
import datetime  
import sqlite3 
import utm
import logging
import openpyxl
import xlsxwriter
from bokeh.charts import Scatter, Line,Bar, show, output_file
from bokeh.io import output_notebook
output_notebook()

# Set up a log
logger = logging.getLogger('notebook')
logger.setLevel('INFO')
nb_root_logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s'                              '- %(message)s',datefmt='%d %b %Y %H:%M:%S')
nb_root_logger.handlers[0].setFormatter(formatter)

# Create input and output folders if they don't exist
os.makedirs('input/original_data', exist_ok=True)
os.makedirs('output', exist_ok=True)
os.makedirs('output/datapackage_renewables', exist_ok=True)


# In[4]:

# Read data from script Part 1
renewables = pd.read_sql('SELECT* FROM raw_data_output',
                          sqlite3.connect('raw_data.sqlite')
                        )
# Correction of date format (necessary due to SQLite-format)
renewables['start_up_date'] = renewables['start_up_date'
                                        ].astype('datetime64[ns]')
renewables['decommission_date'] = renewables['decommission_date'
                                            ].astype('datetime64[ns]')

renewables.info()


# In[ ]:

# Reorder data frame by start-up date
renewables = renewables.ix[pd.to_datetime(renewables.start_up_date
                                         ).sort_values().index]


# In[ ]:

# Create empty marker column
renewables['comment'] = ""

# Validation criteria (R_1) for source BNetzA
idx_date = renewables[(renewables['start_up_date'] <= '2014-12-31') 
                      & (renewables['source'] == 'BNetzA')].index

renewables['comment'].loc[idx_date] = (renewables['comment'] 
                            + "R_1, ")

# Validation criteria (R_1) for source BNetzA_PV
idx_date_pv = renewables[(renewables['start_up_date'] <= '2014-12-31') 
                    & (renewables['source'] == 'BNetzA_PV')].index

renewables['comment'].loc[idx_date_pv] = (renewables['comment'] 
                            + "R_1, ")

# Validation criteria (R_2)
idx_date_null = renewables[(renewables['start_up_date'].isnull())].index

renewables['comment'].loc[idx_date_null] = (renewables['comment'] 
                            + "R_2, ")

# Validation criteria (R_3)
idx_not_inst = renewables[(renewables['notification_reason']!= 'Inbetriebnahme')
                     & (renewables['source'] == 'BNetzA')].index

renewables['comment'].loc[idx_not_inst] = (renewables['comment'] 
                            + "R_3, ")

# Validation criteria (R_4)
idx_pv_date = renewables[(renewables['start_up_date'] < '1975-01-01') 
                   & (renewables['generation_type'] == 'solar')].index

renewables['comment'].loc[idx_pv_date] = (renewables['comment'] 
                            + "R_4, ")
# Validation criteria (R_5)
idx_nv = renewables[renewables['generation_type'] == '#NV'].index

renewables['comment'].loc[idx_nv] = (renewables['comment'] 
                            + "R_5, ")

# Validation criteria (R_6)
idx_gen = renewables[renewables.generation_type.isnull()].index

renewables['comment'].loc[idx_gen] = (renewables['comment'] 
                            + "R_6, ")

# Validation criteria (R_7)
idx_capacity = renewables[renewables.electrical_capacity.isnull()].index

renewables['comment'].loc[idx_capacity] = (renewables['comment'] 
                            + "R_7, ")


# In[ ]:

# Count entries
renewables.groupby(['comment','source'])['comment'].count()


# In[ ]:

# Locate suspect entires
idx_suspect = renewables[renewables.comment.str.len() >1].index

# Show suspect entries
renewables.loc[idx_suspect].groupby(['comment','source'])['comment'].count()


# In[ ]:

# Summarize capacity and generation type of suspect data
renewables.loc[idx_suspect].groupby(['generation_type'])[
                                   'electrical_capacity'].sum()/1000


# In[ ]:

# create new data frame without suspect entries
renewables_clean = renewables.drop(idx_suspect)

# define column of data frame

df_columns = ['start_up_date','electrical_capacity','generation_type',
              'generation_subtype','thermal_capacity','postcode','city', 
              'address','tso','lon','lat','eeg_id','power_plant_id',
              'voltage_level','decommission_date','comment','source']


# In[ ]:

# create final data frame
renewables_final = renewables.loc[:, df_columns]

renewables_final.reset_index(drop=True)

logger.info('Clean final dataframe from not needed columns')


# In[ ]:

# Show structure of data frame
renewables_final.info()


# In[ ]:

# Group and summarize data frame by generation type ans installed capacity
renewables_final.groupby(['generation_type'])['electrical_capacity'].sum()/1000


# In[ ]:

# Group data frame by remaining comments/markers
renewables_final.groupby(['comment'])['comment'].count()


# In[ ]:

# Defining URL
url_bmwi_stat  ='http://www.erneuerbare-energien.de/EE/Redaktion/DE/'                 'Downloads/zeitreihen-zur-entwicklung-der-erneuerbaren-'                 'energien-in-deutschland-1990-2015-excel.xlsx;jsessionid='                 'FFE958ADA709DCBFDD437C8A8FF7D90B?__blob=publicationFile&v=6'

# Reading BMWi data
bmwi_stat = pd.ExcelFile(url_bmwi_stat)   
bmwi_stat = bmwi_stat.parse('4', skiprows=7, skip_footer=8)

# Transform data frame and set column names
stat = bmwi_stat.T
stat.columns = ['bmwi_hydro', 'bmwi_wind_onshore','bmwi_wind_offshore',
                'bmwi_solar','bmwi_biomass','bmwi_biomass_liquid',
                'bmwi_biomass_gas','bmwi_sewage_gas', 'bmwi_landfill_gas',
                'bmwi_geothermal','bmwi_total']

# Drop Null column and set index as year
stat = stat.drop(stat.index[[0]])
stat.index = pd.to_datetime(stat.index,format="%Y").year


# In[ ]:

# Set generation types of interest
generation_types = ['biomass','wind','solar','gas','geothermal','hydro']

# Set date range of time series
idx_stat = pd.date_range(start='1990-01-01', end='2015-01-01', freq='A')
idx_ts = pd.date_range(start='2005-01-01', end='2016-01-31', freq='D')

# Set range of time series as index
data = pd.DataFrame(index=idx_ts)
data_stat = pd.DataFrame(index=idx_stat)

# Create cumulated time series per generation type for both time series
for gtype in generation_types:
    
    temp = renewables_clean[['start_up_date','electrical_capacity'
                           ]].loc[renewables_clean['generation_type'].isin(
                                   [gtype])]
    
    temp_ts = temp.set_index('start_up_date')
    
    # Create cumulated time series per generation type and year
    data_stat['capacity_{0}_de'.format(gtype)]  = (
    temp_ts.resample('A', how='sum').cumsum().fillna(method='ffill')/1000)
    
    # Create cumulated time series per generation type and day
    data['capacity_{0}_de'.format(gtype)] = temp_ts.resample('D',
                                   how='sum').cumsum().fillna(method='ffill')/1000
    # Set index name
    data.index.name = 'timestamp'
    
data_stat.index = pd.to_datetime(data_stat.index,format="%Y").year


# In[ ]:

valuation = pd.concat([data_stat, stat], axis=1)
valuation = valuation.fillna(0)

# Calculate absolute deviation for each year and generation type

valuation['absolute_wind'] =(valuation['capacity_wind_de']
                                 -(valuation['bmwi_wind_onshore'] 
                                 +valuation['bmwi_wind_offshore'])).fillna(0)

valuation['absolute_solar'] =(valuation['capacity_solar_de']
                                  -valuation['bmwi_solar']).fillna(0)

valuation['absolute_hydro'] =(valuation['capacity_hydro_de']
                                  -valuation['bmwi_hydro']).fillna(0)

valuation['absolute_geothermal'] =(valuation['capacity_geothermal_de']
                                  -valuation['bmwi_geothermal']).fillna(0)

valuation['absolute_biomass'] =(valuation['capacity_biomass_de']
                                 -(valuation['bmwi_biomass'] 
                                  +valuation['bmwi_biomass_liquid']
                                  +valuation['bmwi_biomass_gas'])).fillna(0)

valuation['absolute_gas'] =(valuation['capacity_gas_de']
                                 -(valuation['bmwi_sewage_gas'] 
                                  +valuation['bmwi_landfill_gas'])).fillna(0)

valuation['absolute_total'] =((valuation['capacity_biomass_de']
                              +valuation['capacity_wind_de']
                              +valuation['capacity_solar_de']
                             +valuation['capacity_gas_de']
                             +valuation['capacity_geothermal_de']
                             +valuation['capacity_hydro_de']
                           ) -(valuation['bmwi_total'] )).fillna(0)


# In[ ]:

#Plot settings for absolute deviation
deviation_columns = ['absolute_wind','absolute_solar','absolute_hydro',  
                  'absolute_biomass','absolute_gas', 'absolute_total',
                  'absolute_geothermal']

dataplot = valuation[deviation_columns]

deviation = Line(dataplot, 
                 y = deviation_columns,
                 dash = deviation_columns,
                 color = deviation_columns,
            title="Deviation between data set and BMWI statistic", 
            ylabel='Deviation in MW', 
            xlabel='From 1990 till 2014',
            legend=True)


# In[ ]:

# Show Plot for absolute deviation
show(deviation)


# In[ ]:

# Relative deviation
valuation['relative_wind'] =(valuation['absolute_wind']
                            /(valuation['bmwi_wind_onshore'] 
                             +valuation['bmwi_wind_offshore'])).fillna(0)

valuation['relative_solar'] =(valuation['absolute_solar']
                            /(valuation['bmwi_solar'] )).fillna(0)

valuation['relative_hydro'] =(valuation['absolute_hydro']
                            /(valuation['bmwi_hydro'] )).fillna(0)

valuation['relative_geothermal'] =(valuation['absolute_geothermal']
                            /(valuation['bmwi_geothermal'])).fillna(0)

valuation['relative_biomass'] =(valuation['absolute_biomass']
                            /(valuation['bmwi_biomass'] )).fillna(0)

valuation['relative_gas'] =(valuation['absolute_gas']
                            /(valuation['bmwi_sewage_gas'] 
                             +valuation['bmwi_landfill_gas'])).fillna(0)

valuation['relative_total'] =(valuation['absolute_total']
                            /(valuation['bmwi_total'] )).fillna(0)


# In[ ]:

# Plot settings relative deviation
relative_column = ['relative_wind','relative_solar','relative_hydro',
            'relative_biomass','relative_gas','relative_total']

dataplot2 = valuation[relative_column]

relative = Line(dataplot2*100, 
            y = relative_column,
            dash = relative_column,
            color = relative_column,
            title="Deviation between data set and BMWI statistic", 
            ylabel='Relative difference in percent', 
            xlabel='From 1990 till 2014',
            legend=True)


# In[ ]:

# Show Plot for relative deviation
show(relative)


# In[ ]:

# write results as Excel file
valuation.to_excel('validation_report.xlsx', sheet_name='Capacities_1990_2014'
                   )


# In[ ]:

path_package = 'output/datapackage_renewables'

os.makedirs(path_package, exist_ok=True)

# Wirte the results as csv
renewables_final.to_csv(path_package+'/renewable_power_plants_germany.csv',
                         sep=',' , 
                         decimal='.', 
                         date_format='%Y-%m-%d',
                         encoding='utf-8',
                         index = False,
                         if_exists="replace")


# In[ ]:

# Read csv of Marker Explanations
validation = pd.read_csv('input/validation_marker.csv',
                         sep = ',', header = 0)


# In[ ]:

# Write the results as xlsx file
writer = pd.ExcelWriter(path_package+'/renewable_power_plants_germany.xlsx', 
                        engine='xlsxwriter')

# Because of the large number of entries we need to splite the data into two sheets
# (they don't fit on one single Excel sheet)
renewables_final[:1000000].to_excel(writer, 
                                     index = False,
                                    sheet_name='part-1')

renewables_final[1000000:].to_excel(writer, 
                                    index = False,
                                    sheet_name='part-2')

# The explanation of validation markers is added as a sheet
validation.to_excel(writer,
                    index = False,
                    sheet_name='validation_marker')

# Close the Pandas Excel writer and output the Excel file.
writer.save()    


# In[ ]:

# Write the results to sqlite database
renewables_final.to_sql('renewable_power_plants_germany', 
                         sqlite3.connect(path_package+
                                 '/renewable_power_plants_germany.sqlite'),
                         if_exists="replace") 


# In[ ]:

# Write daily cumulated time series as csv
data.to_csv(path_package+'/renewable_capacity_germany_timeseries.csv',
                         sep=',', decimal='.', 
                         date_format='%Y-%m-%dT%H:%M:%S%z',
                         encoding='utf-8',
                         if_exists="replace")


# In[ ]:

# The meta data follows the specification at:
# http://dataprotocols.org/data-packages/

metadata = """
name: opsd-renewable-energy-power-plants
title: List of renewable energy power plants in Germany
description: >-
    This data package contains a list of all renewable energy power plants in Germany 
    that are eligible under the renewable support scheme. For each plant, 
    commissioning data, technical characteristics, and geolocations are provided. 
    It also contains a time series of cumulated installed capacity by technology 
    in daily granularity. The data stem from two different sources: 
    Netztransparenz.de, a joint platform of the German transmission system operators, 
    and Bundesnetzagentur, the regulator. The data has been extracted, merged, 
    verified and cleaned. This processing is documented step-by-step in the script linked below.  
version: "2016-06-07"
keywords: [master data register,power plants,renewables,germany]
geographical-scope: Germany
resources:
    - path: renewable_power_plants_germany.csv
      format: csv
      mediatype: text/csv
      schema:         
          fields:
            - name: start_up_date
              description: Date of start up/installation date
              type: datetime
              format: YYYY-MM-DDThh:mm:ssZ
            - name: electrical_capacity
              description: Installed electrical capacity in kW
              type: number
              format: float
              unit: kW
            - name: generation_type
              description: Type of generation / energy source
              type: string
            - name: generation_subtype
              description: Subtype of generation / energy source
              type: string
            - name: thermal_capacity
              description: Installed thermal capacity in kW
              type: number
              format: float
              unit: kW
            - name: city
              description: Name of location
              type: string
            - name: tso
              description: Name of TSO  
              type: string    
            - name: lon
              description: Longitude coordinates
              type: geopoint
              format: lon
            - name: lat
              description: Latitude coordinates 
              type: geopoint
              format: lat
            - name: eeg_id
              description: EEG (German feed-in tariff law) remuneration number
              type: string
            - name: power_plant_id
              description: Power plant identification number by BNetzA
              type: string
            - name: voltage_level
              description: Voltage level of grid connection
              type: string 
            - name: decommission_date
              description: Date of decommission
              type: datetime
              format: YYYY-MM-DDThh:mm:ssZ  
            - name: comment
              description: Validation comments
              type: string 
            - name: source
              description: Source of database entry
              type: string
              source: TransnetBW, TenneT, Amprion, 50Hertz, BNetzA_PV, BNetzA
    - path: renewable_capacity_germany_timeseries.csv
      format: csv
      mediatype: text/csv
      schema:         
          fields:
            - name: timestamp
              description: Start time of the day
              type: datetime
              format: YYYY-MM-DDThh:mm:ssZ
            - name: capacity_biomass_de
              description: Cumulated biomass electrical capacity
              type: number
            - name: capacity_wind_de
              description: Cumulated wind capacity
              type: number                 
            - name: capacity_solar_de
              description: Cumulated solar capacity
              type: number                
            - name: capacity_gas_de
              description: Cumulated gas electrical capacity
              type: number  
            - name: capacity_geothermal_de
              description: Cumulated geothermal electrical capacity
              type: number 
            - name: capacity_hydro_de
              description: Cumulated hydro capacity
              type: number  
    - path: renewable_power_plants_germany.xlsx
      format: xlsx
      mediatype: xlsx
      schema:         
          fields:
            - name: start_up_date
              description: Date of start up/installation date
              type: datetime
              format: YYYY-MM-DDThh:mm:ssZ
            - name: electrical_capacity
              description: Installed electrical capacity in kW
              type: number
              format: float
              unit: kW
            - name: generation_type
              description: Type of generation / energy source
              type: string
            - name: generation_subtype
              description: Subtype of generation / energy source
              type: string
            - name: thermal_capacity
              description: Installed thermal capacity in kW
              type: number
              format: float
              unit: kW
            - name: city
              description: Name of location
              type: string
            - name: tso
              description: Name of TSO  
              type: string    
            - name: lon
              description: Longitude coordinates
              type: geopoint
              format: lon
            - name: lat
              description: Latitude coordinates 
              type: geopoint
              format: lat
            - name: eeg_id
              description: EEG (German feed-in tariff law) remuneration number
              type: string
            - name: power_plant_id
              description: Power plant identification number by BNetzA
              type: string
            - name: voltage_level
              description: Voltage level of grid connection
              type: string 
            - name: decommission_date
              description: Date of decommission
              type: datetime
              format: YYYY-MM-DDThh:mm:ssZ  
            - name: comment
              description: Validation comments
              type: string 
            - name: source
              description: Source of database entry
              type: string
              source: TransnetBW, TenneT, Amprion, 50Hertz, BNetzA_PV, BNetzA
licenses:
    - url: http://example.com/license/url/here
      name: License Name Here
      version: 1.0
      id: license-id-from-open
sources:
    - name: Bundesnetzagentur - register of renewable power plants (excl. PV)
      web: http://www.bundesnetzagentur.de/cln_1422/DE/Sachgebiete/ElektrizitaetundGas/Unternehmen_Institutionen/ErneuerbareEnergien/Anlagenregister/Anlagenregister_Veroeffentlichung/Anlagenregister_Veroeffentlichungen_node.html
      source: BNetzA
    - name: Bundesnetzagentur - register of PV power plants
      web: http://www.bundesnetzagentur.de/cln_1431/DE/Sachgebiete/ElektrizitaetundGas/Unternehmen_Institutionen/ErneuerbareEnergien/Photovoltaik/DatenMeldgn_EEG-VergSaetze/DatenMeldgn_EEG-VergSaetze_node.html    
      source: BNetzA_PV
    - name: Netztransparenz.de - information platform of German TSOs (register of renewable power plants in their control area)
      web: https://www.netztransparenz.de/de/Anlagenstammdaten.htm
      source: TransnetBW, TenneT, Amprion, 50Hertz
    - name: Postleitzahlen Deutschland - zip codes of Germany linked to geo-information
      web: http://www.suche-postleitzahl.org/downloads
maintainers:
    - name: Frauke Wiese
      email: frauke.wiese@uni-flensburg.de
      web: http://open-power-system-data.org/
views: True
openpowersystemdata-enable-listing: True
opsd-jupyter-notebook-url: https://github.com/Open-Power-System-Data/datapackage_renewable_power_plants/blob/master/main.ipynb
opsd-changes-to-last-version: Update of output data (latest version BNetzA-data, suspect data is not deleted any more but marked), corrected minor bugs of format and description
"""

metadata = yaml.load(metadata)

datapackage_json = json.dumps(metadata, indent=4, separators=(',', ': '))

# Write the information of the metadata
with open(os.path.join(path_package, 'datapackage.json'), 'w') as f:
    f.write(datapackage_json)

