# remove markdown cells

# coding: utf-8

# In[3]:

# importing all necessary Python libraries for this Script
# %matplotlib inline

import os
import zipfile
import posixpath
import urllib.parse
import urllib.request
import numpy as np
import pandas as pd
import sqlite3 
import utm
import logging

# Set up a log
logger = logging.getLogger('notebook')
logger.setLevel('INFO')
nb_root_logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s- %(message)s', datefmt='%d %b %Y %H:%M:%S')
# nb_root_logger.handlers[0].setFormatter(formatter)

# Create input and output folders if they don't exist
os.makedirs('input/original_data', exist_ok=True)
os.makedirs('output', exist_ok=True)
os.makedirs('output/datapackage_renewables', exist_ok=True)


# In[ ]:

# point URLs to original data
url_netztransparenz = 'https://www.netztransparenz.de/de/file/'                  'Anlagenstammdaten_2014_4UeNB.zip'

url_bnetza ='http://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/'            'Energie/Unternehmen_Institutionen/ErneuerbareEnergien/Anlagenregister/'            'VOeFF_Anlagenregister/2016_04_Veroeff_AnlReg.xls?__blob=publicationFile&v=2'
        
url_bnetza_pv = 'https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/'                'Sachgebiete/Energie/Unternehmen_Institutionen/'                'ErneuerbareEnergien/Photovoltaik/Datenmeldungen/'                'Meldungen_Aug-Mrz2016.xls?__blob=publicationFile&v=2'


# In[ ]:

def downloadandcache(url):
    """This function downloads a file into a folder called 
    original_data and returns the local filepath."""
    
    path = urllib.parse.urlsplit(url).path
    filename = posixpath.basename(path)
    filepath = "input/original_data/"+filename
    
    #check if file exists, if not download it
    if not os.path.exists(filepath):
        print("Downloading file", filename)
        urllib.request.urlretrieve(url, filepath)
    else:
        print("Using local file from", filepath)
    filepath = ''+filepath
    return filepath


# In[ ]:

# load zip file for data from netztransparenz.de
z = zipfile.ZipFile(downloadandcache(url_netztransparenz))

# Get TSO data from zip file
amprion_df = pd.read_csv(z.open('Amprion_Anlagenstammdaten_2014.csv'),
                       sep=';',        
                       thousands='.', 
                       decimal=',',    
                       header=0,
                       parse_dates=[11,12,13,14], 
                       encoding = 'cp850',
                       dayfirst=True, 
                       low_memory=False)

hertz_df = pd.read_csv(z.open('50Hertz_Anlagenstammdaten_2014.csv'),
                       sep=';',        
                       thousands='.', 
                       decimal=',',   
                       header=0,
                       parse_dates=[11,12,13,14],  
                       encoding = 'cp1252',
                       dayfirst=True, 
                       low_memory=False)

tennet_df = pd.read_csv(z.open('TenneT_Anlagenstammdaten_2014.csv'),
                       sep=';',        
                       thousands='.',  
                       decimal=',',    
                       header=0,
                       parse_dates=[11,12,13,14], 
                       encoding = 'cp1252',
                       dayfirst=True, 
                       low_memory=False)

transnetbw_df = pd.read_csv(z.open('TransnetBW_Anlagenstammdaten_2014.csv'),
                       sep=';',       
                       thousands='.',  
                       decimal=',',    
                       header=0,
                       parse_dates=[11,12,13,14], 
                       encoding = 'cp1252',
                       dayfirst=True, 
                       low_memory=False)

# Get BNetzA register 
bnetza_df = pd.read_excel(downloadandcache(url_bnetza),
                   sheetname='Gesamtübersicht',
                   header=0,
                   converters={'4.9 Postleit-zahl':str})

# Get BNetzA-PV register
bnetza_pv = pd.ExcelFile(downloadandcache(url_bnetza_pv))

# Combine all PV BNetzA sheets into one data frame
bnetza_pv_df = pd.concat(bnetza_pv.parse(sheet, skiprows=10,
                          converters={'Anlage \nPLZ':str}
                         ) for sheet in bnetza_pv.sheet_names)

# Drop not needed NULL "Unnamed:" column
bnetza_pv_df = bnetza_pv_df.drop(bnetza_pv_df.columns[[7]], axis=1)


# In[ ]:

# Get translation list
columnnames =pd.read_csv('input/column_translation_list.csv',sep = ",",
                           header=0)

columndict = columnnames.set_index('original_name')['column_naming'].to_dict()

# Translate columns by list 
bnetza_pv_df.rename(columns = columndict , inplace=True)
bnetza_df.rename(columns = columndict , inplace=True)
transnetbw_df.rename(columns = columndict , inplace=True)
tennet_df.rename(columns = columndict , inplace=True)
amprion_df.rename(columns = columndict , inplace=True)
hertz_df.rename(columns = columndict , inplace=True)

# Translate special cases separately
backslash ={'Anlage \nBundesland': 'federal_state','Anlage \nOrt oder Gemarkung': 
            'city','Anlage \nPLZ': 'postcode','Anlage \nStraße oder Flurstück *)': 
            'address', 'Installierte \nNennleistung [kWp]': 'electrical_capacity'}

bnetza_pv_df.rename(columns = backslash, inplace=True)


# In[ ]:

# Add source names to the data frames
transnetbw_df['source'] = 'TransnetBW'
tennet_df['source'] = 'TenneT'
amprion_df['source'] = 'Amprion'
hertz_df['source'] = '50Hertz'
bnetza_df['source'] = 'BNetzA'
bnetza_pv_df['source'] = 'BNetzA_PV'

# Add for the BNetzA PV data the generation types
bnetza_pv_df['generation_type'] = 'solar'
bnetza_pv_df['generation_subtype'] = 'solar_roof_mounted'


# In[ ]:

# Merge data frames
dataframes = [transnetbw_df,tennet_df, amprion_df, hertz_df, 
             bnetza_pv_df,bnetza_df]

renewables = pd.concat(dataframes)

renewables = renewables.reset_index()


# In[ ]:

# Transfer generation_type values to generation_subtype
idx_subtype = renewables[(renewables['source'] != 'BNetzA_PV')].index

renewables['generation_subtype'].loc[idx_subtype] = (
                                 renewables['generation_type'].loc[idx_subtype])


# In[ ]:

column_interest = ['start_up_date', 'electrical_capacity','generation_type',
                   'generation_subtype','thermal_capacity','city', 'postcode',
                   'address','tso','dso' ,'utm_zone','utm_east', 'utm_north',
                   'notification_reason', 'eeg_id',
                   'voltage_level','decommission_date',
                   'power_plant_id','source']


# In[ ]:

renewables = renewables.loc[:, column_interest]
renewables.reset_index(drop=True)
logger.info('Clean dataframe from not needed columns')


# In[ ]:

renewables.info()


# In[ ]:

# Read translation list
translation_list =pd.read_csv('input/value_translation_list.csv',sep = ",",
                           header=0)
# Create dictionnary in order to change values 
translation_dict = translation_list.set_index('original_name')['opsd_naming'].to_dict()

types_list =pd.read_csv('input/generation_types_translation_list.csv',sep = ",",
                           header=0)

types_dict = types_list.set_index('generation_subtype')['generation_type'].to_dict()


# In[ ]:

# Running time ~ 10 min
renewables.replace(translation_dict, inplace=True)

# Create new column for generation_subtype
renewables['generation_subtype'] = renewables.generation_type

# Replace generation_subtype by generation_type
renewables.generation_type.replace(types_dict, inplace=True)


# In[ ]:

# Electrical capacity per generation type (in MW)
renewables.groupby(['generation_type'])['electrical_capacity'].sum() / 1000


# In[ ]:

# Electrical capacity per generation subtype (in MW)
renewables.groupby(['generation_subtype'])['electrical_capacity'].sum() / 1000


# In[ ]:

# Read generated postcode/location file
postcode = pd.read_csv('input/de_tso_postcode_gps.csv',
                       sep=';',
                       header=0)

# Drop possible duplicates in postcodes
postcode.drop_duplicates('postcode', keep='last',inplace=True)

# Show first entries
postcode.head()


# In[ ]:

# Take postcode and longitude/latitude infomations
postcode= postcode[[0,3,4]]

renewables =renewables.merge(postcode, on=['postcode'],  how='left')


# In[ ]:

renewables.groupby(['utm_zone'])['utm_zone'].count()


# In[ ]:

# Find entries with 32 value at the beginning
ix_32 = (renewables['utm_east'].astype(str).str[:2]=='32')
ix_notnull= renewables['utm_east'].notnull()

# Remove 32 from utm_east entries
renewables.loc[ix_32,'utm_east'] = renewables.loc[ix_32,'utm_east'
                ].astype(str).str[2:].astype(float)


# In[ ]:

# Convert from UTM values to latitude and longitude coordinates
try:
    
    renewables['lonlat'] = renewables.loc[ix_notnull,['utm_east',
    'utm_north','utm_zone']].apply(lambda x: utm.to_latlon(x[0],
    x[1],x[2],'U'), axis=1).astype(str)
    
except:
    
    renewables['lonlat'] = np.NaN
    
lat = []
lon = []

for row in renewables['lonlat']:
    try:
        # Split tuple format
        # into the column lat and lon  
        row = row.lstrip('(').rstrip(')')
        lat.append(row.split(',')[0])
        lon.append(row.split(',')[1])
    except:
        # set NAN 
        lat.append(np.NaN)
        lon.append(np.NaN)
          

renewables['latitude'] = lat
renewables['longitude'] = lon    

# Add new values to data frame lon and lat
renewables['lon'] = renewables[['longitude','lon']].apply(lambda x: x[1] 
                        if pd.isnull(x[0]) else x[0], axis=1)

renewables['lat'] = renewables[['latitude','lat']].apply(lambda x: x[1] 
                        if pd.isnull(x[0]) else x[0], axis=1)


# In[ ]:

print('Missing Coordinates ',renewables.lat.isnull().sum())

renewables[renewables.lat.isnull()].groupby(['generation_type',
                                           'source']
                                          )['source'].count()


# In[ ]:

renewables.info()


# In[ ]:

renewables.to_sql('raw_data_output', 
                 sqlite3.connect('raw_data.sqlite')
                 , if_exists="replace")   

