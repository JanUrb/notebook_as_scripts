
# coding: utf-8

# <table style="width:100%; background-color: #D9EDF7">
#   <tr>
#     <td style="border: 1px solid #CFCFCF">
#       <b>Renewable power plants: Download and process Notebook</b>
#       <ul>
#         <li><a href="main.ipynb">Main Notebook</a></li>
#         <li>Download and process Notebook</li>
#         <li><a href="validation_and_output.ipynb">Validation and output Notebook</a></li>
#       </ul>
#       <br>This Notebook is part of the <a href="http://data.open-power-system-data.org/renewable_power_plants"> Renewable power plants Data Package</a> of <a href="http://open-power-system-data.org">Open Power System Data</a>.
#     </td>
#   </tr>
# </table>

# This script downlads and extracts the original data of renewable power plant lists from the data sources, processes and merges them. It subsequently adds the geolocation for each power plant. Finally it saves the DataFrames as pickle-files. Make sure you run the download and process Notebook before the validation and output Notebook.

# # Table of contents 
# 
# * [1. Script setup](#1.-Script-setup)
# * [2. Settings](#2.-Settings)
#     * [2.1 Choose download option](#2.1-Choose-download-option)
#     * [2.2 Download function](#2.2-Download-function)
#     * [2.3 Setup translation dictionaries](#2.3-Setup-translation-dictionaries)
# * [3. Download and process per country](#3.-Download-and-process-per-country)
#     * [3.1 Germany DE](#3.1-Germany-DE)
#         * [3.1.1 Download and read](#3.1.1-Download-and-read)
#         * [3.1.2 Translate column names](#3.1.2-Translate-column-names)
#         * [3.1.3 Add information and choose columns](#3.1.3-Add-information-and-choose-columns)
#         * [3.1.4 Merge DataFrames](#3.1.4-Merge-DataFrames)
#         * [3.1.5 Translate values and harmonize energy source](#3.1.5-Translate-values-and-harmonize-energy-source)
#         * [3.1.6 Transform electrical_capacity from kW to MW](#3.1.6-Transform-electrical_capacity-from-kW-to-MW)
#         * [3.1.7 Georeferencing](#3.1.7-Georeferencing)
#         * [3.1.8 Save](#3.1.8-Save)
#     * [3.2 Denmark DK](#3.2-Denmark-DK)
#         * [3.2.1 Download and read](#3.2.1-Download-and-read)
#         * [3.2.2 Translate column names](#3.2.2-Translate-column-names)
#         * [3.2.3 Add data source and missing information](#3.2.3-Add-data-source-and-missing-information)
#         * [3.2.4 Translate values and harmonize energy source](#3.2.4-Translate-values-and-harmonize-energy-source)
#         * [3.2.5 Georeferencing](#3.1.5-Georeferencing)
#         * [3.2.6 Merge DataFrames and choose columns](#3.2.6-Merge-DataFrames-and-choose-columns)
#         * [3.1.7 Transform electrical_capacity from kW to MW](#3.1.7-Transform-electrical_capacity-from-kW-to-MW)
#         * [3.2.8 Save](#3.1.8-Save)
#     * [3.3 France FR](#3.3-France-FR)
#         * [3.3.1 Download and read](#3.3.1-Download-and-read)
#         * [3.3.2 Rearrange columns and translate column names](#3.3.2-Rearragne-columns-and-translate-column-names)
#         * [3.3.3 Add data source](#3.3.3-Add-data-source)
#         * [3.3.4 Translate values and harmonize energy source](#3.3.4-Translate-values-and-harmonize-energy-source)
#         * [3.3.5 Georeferencing](#3.3.5-Georeferencing)
#         * [3.3.6 Save](#3.3.6-Save)
#     * [3.4 Poland PL](#3.4-Poland-PL)
#         * [3.4.1 Download and read](#3.4.1-Download-and-read)
#         * [3.4.2 Rearrange data from rtf-file](#3.4.2-Rearrange-data-from-rtf-file)
#         * [3.4.3 Add data source](#3.4.3-Add-data-source)
#         * [3.4.4 Translate values and harmonize energy source](#3.4.4-Translate-values-and-harmonize-energy-source)
#         * [3.4.5 Georeferencing -_work in progress_](#3.4.6-Georeferencing---work-in-progress)
#         * [3.4.6 Save](#3.4.7-Save)
# * [Part 2: Validation and output](validation_and_output.ipynb)
# 

# # 1. Script setup

# In[42]:

# importing all necessary Python libraries for this Script

from collections import OrderedDict
import io
import json
import os
import subprocess
import zipfile
import posixpath
import urllib.parse
import urllib.request
import numpy as np
import pandas as pd
import requests 
import sqlite3 
import logging
import getpass
import utm # for transforming geoinformation in the utm-format
import re # provides regular expression matching operations

# Starting from ipython 4.3.0 logging is not directing its ouput to the out cell. It might be operating system related but 
# until the issue is fixed, we are going to use print(). 
# Issue on GitHub: https://github.com/ipython/ipykernel/issues/111

# Set up a log 
logging.basicConfig(handlers=[logging.StreamHandler()])
logger = logging.getLogger('notebook')
logger.setLevel('INFO')
nb_root_logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s'                              '- %(message)s',datefmt='%d %b %Y %H:%M:%S')

# Create input and output folders if they don't exist
os.makedirs('input/original_data', exist_ok=True)
os.makedirs('output', exist_ok=True)
os.makedirs('output/renewable_power_plants', exist_ok=True)


# # 2. Settings

# ## 2.1 Choose download option
# The original data can either be downloaded from the original data sources as specified below or from the opsd-Server. Default option is to download from the original sources as the aim of the project is to stay as close to original sources as possible. However, if problems with downloads e.g. due to changing urls occur, you can still run the script with the original data from the opsd_server.

# In[43]:

download_from = 'original_sources'
# download_from = 'opsd_server' 


# In[44]:

if download_from == 'opsd_server':

# While OPSD is in beta, we need to supply authentication
    password = getpass.getpass('Please enter the beta user password:')
    session = requests.session()
    session.auth = ('beta', password) 

# Specify direction to original_data folder on the opsd data server
    url_opsd = 'http://data.open-power-system-data.org/renewables_power_plants/'
    version = '2016-08-25'
    folder = '/original_data'


# ## 2.2 Download function

# In[45]:

def download_and_cache(url, session=None):
    """This function downloads a file into a folder called 
    original_data and returns the local filepath."""
    path = urllib.parse.urlsplit(url).path
    filename = posixpath.basename(path)
    filepath = "input/original_data/" + filename
    print(url)

    # check if file exists, if not download it
    filepath = "input/original_data/" + filename
    print(filepath)
    if not os.path.exists(filepath):
        if not session:
            print('No session')
            session = requests.session()
        
        print("Downloading file: ", filename)
        r = session.get(url, stream=True)

        chuncksize = 1024
        with open(filepath, 'wb') as file:
            for chunck in r.iter_content(chuncksize):
                file.write(chunck)
    else:
        print("Using local file from", filepath)
    filepath = '' + filepath
    return filepath


# ## 2.3 Setup translation dictionaries
# 
# Column and value names of the original data sources will be translated to English and standardized across different sources. Standardized column names, e.g. "electrical_capacity" are required to merge data in one DataFrame.<br>
# The column and the value translation lists are provided in the input folder of the Data Package.

# In[46]:

# Get column translation list
columnnames = pd.read_csv('input/column_translation_list.csv')


# In[47]:

# Get value translation list
valuenames = pd.read_csv('input/value_translation_list.csv')


# # 3. Download and process per country
# 
# For one country after the other, the original data is downloaded, read, processed, translated, eventually georeferenced and saved. If respective files are already in the local folder, these will be utilized.
# To process the provided data [pandas DataFrame](http://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe) is applied.<br>

# ## 3.1 Germany DE

# ### 3.1.1 Download and read
# The data which will be processed below is provided by the following data sources:
# 
# **[Netztransparenz.de](https://www.netztransparenz.de/de/Anlagenstammdaten.htm)** - Official grid transparency platform from the German TSOs (50Hertz, Amprion, TenneT and TransnetBW).
# 
# **Bundesnetzagentur (BNetzA)** - German Federal Network Agency for Electricity, Gas, Telecommunications, Posts and Railway (Data for [roof-mounted PV power plants](http://www.bundesnetzagentur.de/cln_1422/DE/Sachgebiete/ElektrizitaetundGas/Unternehmen_Institutionen/ErneuerbareEnergien/Photovoltaik/DatenMeldgn_EEG-VergSaetze/DatenMeldgn_EEG-VergSaetze_node.html) and for [all other renewable energy power plants](http://www.bundesnetzagentur.de/cln_1412/DE/Sachgebiete/ElektrizitaetundGas/Unternehmen_Institutionen/ErneuerbareEnergien/Anlagenregister/Anlagenregister_Veroeffentlichung/Anlagenregister_Veroeffentlichungen_node.html))

# In[48]:

# point URLs to original data depending on the chosen download option
if download_from == 'original_sources':
     
    url_netztransparenz ='https://www.netztransparenz.de/de/file/Anlagenstammdaten_2015_final.zip'  
    url_bnetza ='http://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/ErneuerbareEnergien/Anlagenregister/VOeFF_Anlagenregister/2016_06_Veroeff_AnlReg.xls?__blob=publicationFile&v=1'
    url_bnetza_pv = 'https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/ErneuerbareEnergien/Photovoltaik/Datenmeldungen/Meldungen_Aug-Mai2016.xls?__blob=publicationFile&v=2'
    
elif download_from == 'opsd_server':
    
    url_netztransparenz = (url_opsd + version + folder + '/Netztransparenz/' + 'Anlagenstammdaten_2015_final.zip')
    url_bnetza = (url_opsd + version + folder + '/BNetzA/' + '2016_06_Veroeff_AnlReg.xls')
    url_bnetza_pv = (url_opsd + version + folder + '/BNetzA/' + 'Meldungen_Aug-Mai2016.xls')


# In[49]:

# Download all data sets before processing.
if download_from == 'original_sources':
    
    netztransparenz_zip = get_ipython().magic('time zipfile.ZipFile(download_and_cache(url_netztransparenz))')
    bnetza_xls = get_ipython().magic('time download_and_cache(url_bnetza)')
    bnetza_pv_xls = get_ipython().magic('time download_and_cache(url_bnetza_pv)')

elif download_from == 'opsd_server':
    # Check if the user is offline
    # if offline, do not give a session as parameter.
    try:
        online = True
        r = session.get('http://data.open-power-system-data.org/renewables_power_plants/')
    except requests.ConnectionError:
        logger.warning('The user is offline. Proceeding with the script!')
        
        
    try:     
        netztransparenz_zip = get_ipython().magic('time zipfile.ZipFile(download_and_cache(url_netztransparenz, session))')
        bnetza_xls = get_ipython().magic('time download_and_cache(url_bnetza, session)')
        bnetza_pv_xls = get_ipython().magic('time download_and_cache(url_bnetza_pv, session)')
    except zipfile.BadZipFile:
        raise FileNotFoundError('One of the Zip File is corrupted! Delete them                                  Also, check your opsd password!')


# In[50]:

# Read TSO data from zip file
print('Reading Amprion_Anlagenstammdaten_2015.csv')
amprion_df = pd.read_csv(netztransparenz_zip.open('Amprion_Anlagenstammdaten_2015.csv'),
                         sep=';',
                         thousands='.',
                         decimal=',',
                         header=0,
                         parse_dates=[11, 12, 13, 14],
                         encoding='cp1252',
                         dayfirst=True)

print('Reading 50Hertz_Anlagenstammdaten_2015.csv')
hertz_df = pd.read_csv(netztransparenz_zip.open('50Hertz_Anlagenstammdaten_2015.csv'),
                       sep=';',
                       thousands='.',
                       decimal=',',
                       header=0,
                       parse_dates=[11, 12, 13, 14],
                       encoding='cp1252',
                       dayfirst=True)

print('Reading TenneT_Anlagenstammdaten_2015.csv')
tennet_df = pd.read_csv(netztransparenz_zip.open('TenneT_Anlagenstammdaten_2015.csv'),
                        sep=';',
                        thousands='.',
                        decimal=',',
                        header=0,
                        parse_dates=[11, 12, 13, 14],
                        encoding='cp1252',
                        dayfirst=True)

print('Reading TransnetBW_Anlagenstammdaten_2015.csv')
transnetbw_df = pd.read_csv(netztransparenz_zip.open('TransnetBW_Anlagenstammdaten_2015.csv'),
                            sep=';',
                            thousands='.',
                            decimal=',',
                            header=0,
                            parse_dates=[11, 12, 13, 14],
                            encoding='cp1252',
                            dayfirst=True,
                            low_memory=False)


# In[51]:

# Read BNetzA register
print('Reading bnetza - 2016_06_Veroeff_AnlReg.xls')
bnetza_df = pd.read_excel(bnetza_xls,
                          sheetname='Gesamtübersicht',
                          header=0,
                          converters={'4.9 Postleit-zahl': str,
                                      'Gemeinde-Schlüssel': str})

# Read BNetzA-PV register
print('Reading bnetza_pv - Meldungen_Aug-Mai2016.xls')
bnetza_pv = pd.ExcelFile(bnetza_pv_xls)

# Combine all PV BNetzA sheets into one DataFrame
print('Concatenating bnetza_pv')
bnetza_pv_df = pd.concat(bnetza_pv.parse(sheet, skiprows=10,
                                         converters={'Anlage \nPLZ': str}
                                         ) for sheet in bnetza_pv.sheet_names)

# Drop not needed NULL "Unnamed:" column
bnetza_pv_df = bnetza_pv_df.drop(bnetza_pv_df.columns[[7]], axis=1)


# ### 3.1.2 Translate column names
# To standardise the DataFrame the original column names from the German TSOs and the BNetzA wil be translated and new english column names wil be assigned to the DataFrame. The unique column names are required to merge the DataFrame.<br>
# The column_translation_list is provided here as csv in the input folder. It is loaded in _2.3 Setup of translation dictionaries_.

# In[52]:

# Choose the translation terms for Germany, create dictionary and show dictionary
idx_DE = columnnames[columnnames['country'] == 'DE'].index
column_dict_DE = columnnames.loc[idx_DE].set_index('original_name')['opsd_name'].to_dict()
column_dict_DE


# In[53]:

print('Translation')
amprion_df.rename(columns=column_dict_DE, inplace=True)
hertz_df.rename(columns=column_dict_DE, inplace=True)
tennet_df.rename(columns=column_dict_DE, inplace=True)
transnetbw_df.rename(columns=column_dict_DE, inplace=True)
bnetza_df.rename(columns=column_dict_DE, inplace=True)
bnetza_pv_df.rename(columns=column_dict_DE, inplace=True)


# ### 3.1.3 Add information and choose columns
# All data source names and (for the BNetzA-PV data) the energy source will is added.

# In[54]:

# Add data source names to the DataFrames
transnetbw_df['data_source'] = 'TransnetBW'
tennet_df['data_source'] = 'TenneT'
amprion_df['data_source'] = 'Amprion'
hertz_df['data_source'] = '50Hertz'
bnetza_df['data_source'] = 'BNetzA'
bnetza_pv_df['data_source'] = 'BNetzA_PV'

# Add for the BNetzA PV data the energy source
bnetza_pv_df['energy_source'] = 'Photovoltaics'

# Correct datetime-format
def decom_fkt(x):
    x = str(x)
    if x == 'nan':
        x = ''
    else:
        x = x[0:10]
    return x

bnetza_df['decommissioning_date'] = bnetza_df['decommissioning_date'].apply(
    decom_fkt)


# In[55]:

# Just some of all the columns of this DataFrame are utilized further
bnetza_df = bnetza_df.loc[:,('commissioning_date','decommissioning_date','notification_reason',
                             'energy_source',
                             'electrical_capacity_kW','thermal_capacity_kW',
                             'voltage_level','dso','eeg_id','bnetza_id',
                             'federal_state','postcode','municipality_code','municipality',
                             'address','address_number',
                             'utm_zone','utm_east','utm_north',
                             'data_source')]


# ### 3.1.4 Merge DataFrames
# The individual DataFrames from the TSOs (Netztransparenz.de) and BNetzA are merged.

# In[56]:

dataframes = [transnetbw_df, tennet_df, amprion_df, hertz_df, bnetza_pv_df, bnetza_df]
DE_renewables = pd.concat(dataframes)
# Make sure the decommissioning_column has the right dtype
DE_renewables['decommissioning_date'] = pd.to_datetime(DE_renewables['decommissioning_date'])
DE_renewables.reset_index(drop=True, inplace=True)


# **First look at DataFrame structure and format**

# In[57]:

DE_renewables.info()


# ### 3.1.5 Translate values and harmonize energy source
# Different German terms for energy source, energy source subtypes and voltage levels are translated and harmonized across the individual data sources. The value_translation_list is provided here as csv in the input folder. It is loaded in _2.3 Setup of translation dictionaries_.

# In[58]:

# Choose the translation terms for Germany, create dictionary and show dictionary
idx_DE = valuenames[valuenames['country'] == 'DE'].index
value_dict_DE = valuenames.loc[idx_DE].set_index('original_name')['opsd_name'].to_dict()
value_dict_DE


# In[59]:

print('replacing..')
# Running time: some minutes. %time prints the time your computer required for this step
get_ipython().magic('time DE_renewables.replace(value_dict_DE, inplace=True)')


# **Separate and assign energy source and subtypes**

# In[60]:

# Create dctionnary in order to assign energy_source to its subtype
energy_source_dict_DE = valuenames.loc[idx_DE].set_index('opsd_name')['energy_source'].to_dict()

# Column energy_source partly contains subtype information, thus this column is copied
# to new column for energy_source_subtype...
DE_renewables['energy_source_subtype'] = DE_renewables['energy_source']

# ...and the energy source subtype values in the energy_source column are replaced by 
# the higher level classification
DE_renewables['energy_source'].replace(energy_source_dict_DE, inplace=True)


# In[61]:

# Overview of dictionary
energy_source_dict_DE


# **Summary of DataFrame**

# In[62]:

# Electrical capacity per energy_source (in MW)
DE_renewables.groupby(['energy_source'])['electrical_capacity_kW'].sum() / 1000


# In[63]:

# Electrical capacity per energy_source_subtype (in MW)
DE_renewables.groupby(['energy_source_subtype'])['electrical_capacity_kW'].sum() / 1000


# ### 3.1.6 Transform electrical_capacity from kW to MW

# In[64]:

# kW to MW
DE_renewables[['electrical_capacity_kW','thermal_capacity_kW']] /= 1000

# adapt column name
DE_renewables.rename(columns={'electrical_capacity_kW' : 'electrical_capacity',
                              'thermal_capacity_kW' : 'thermal_capacity'},inplace=True)


# ### 3.1.7 Georeferencing

# #### Get coordinates by postcode
# *(for data with no existing geocoordinates)*
# 
# The available post code in the original data provides a first approximation for the geocoordinates of the RE power plants.<br>
# The BNetzA data provides the full zip code whereas due to data privacy the TSOs only report the first three digits of the power plant's post code (e.g. 024xx) and no address. Subsequently a centroid of the post code region polygon is used to find the coordinates.
# 
# With data from
# *  http://www.suche-postleitzahl.org/downloads?download=plz-gebiete.shp.zip
# *  http://www.suche-postleitzahl.org/downloads?download_file=plz-3stellig.shp.zip
# *  http://www.suche-postleitzahl.org/downloads
# 
# a CSV-file for all existing German post codes with matching geocoordinates has been compiled. The latitude and longitude coordinates were generated by running a PostgreSQL + PostGIS database. Additionally the respective TSO has been added to each post code. *(A Link to the SQL script will follow here later)*
# 
# *(License: http://www.suche-postleitzahl.org/downloads, Open Database Licence for free use. Source of data: © OpenStreetMap contributors)*

# In[65]:

# Read generated postcode/location file
postcode = pd.read_csv('input/de_tso_postcode_gps.csv',
                       sep=';',
                       header=0)

# Drop possible duplicates in postcodes
postcode.drop_duplicates('postcode', keep='last',inplace=True)

# Show first entries
postcode.head()


# ** Merge geometry information by using the postcode**

# In[66]:

# Take postcode and longitude/latitude informations
postcode = postcode[[0,3,4]]

DE_renewables = DE_renewables.merge(postcode, on=['postcode'],  how='left')


# #### Transform geoinformation
# *(for data with already existing geoinformation)*
# 
# In this section the existing geoinformation (in UTM-format) will be transformed into latidude and longitude coordiates as a uniform standard for geoinformation. 
# 
# The BNetzA data set offers UTM Geoinformation with the columns *utm_zone (UTM-Zonenwert)*, *utm_east* and *utm_north*. Most of utm_east-values include the utm_zone-value **32** at the beginning of the number. In order to properly standardize and transform this geoinformation into latitude and longitude it is necessary to remove this utm_zone value. For all UTM entries the utm_zone 32 is used by the BNetzA.
# 
# 
# |utm_zone|	 utm_east|	 utm_north| comment|
# |---|---|---| ----|
# |32|	413151.72|	6027467.73| proper coordinates|
# |32|	**32**912159.6008|	5692423.9664| caused error by 32|
# 

# **How many different utm_zone values are in the data set?**

# In[67]:

DE_renewables.groupby(['utm_zone'])['utm_zone'].count()


# **Remove the utm_zone "32" from the utm_east value**

# In[68]:

# Find entries with 32 value at the beginning
ix_32 = (DE_renewables['utm_east'].astype(str).str[:2] == '32')
ix_notnull = DE_renewables['utm_east'].notnull()

# Remove 32 from utm_east entries
DE_renewables.loc[ix_32,'utm_east'] = DE_renewables.loc[ix_32,'utm_east'].astype(str).str[2:].astype(float)


# **Conversion UTM to lat/lon**

# In[69]:

# Convert from UTM values to latitude and longitude coordinates
try:
    DE_renewables['lonlat'] = DE_renewables.loc[ix_notnull, ['utm_east', 'utm_north', 'utm_zone']].apply(
        lambda x: utm.to_latlon(x[0], x[1], x[2], 'U'),
        axis=1) \
        .astype(str)
    
except:
    DE_renewables['lonlat'] = np.NaN
    
lat = []
lon = []

for row in DE_renewables['lonlat']:
    try:
        # Split tuple format into the column lat and lon  
        row = row.lstrip('(').rstrip(')')
        lat.append(row.split(',')[0])
        lon.append(row.split(',')[1])
    except:
        # set NaN 
        lat.append(np.NaN)
        lon.append(np.NaN)
          

DE_renewables['latitude'] = pd.to_numeric(lat)
DE_renewables['longitude'] = pd.to_numeric(lon) 

# Add new values to DataFrame lon and lat
DE_renewables['lat'] = DE_renewables[['lat', 'latitude']].apply(
    lambda x: x[1] if pd.isnull(x[0]) else x[0],
    axis=1)

DE_renewables['lon'] = DE_renewables[['lon', 'longitude']].apply(
    lambda x: x[1] if pd.isnull(x[0]) else x[0],
    axis=1)


# **Check: missing coordinates by data source and type**

# In[70]:

print('Missing Coordinates ', DE_renewables.lat.isnull().sum())

DE_renewables[DE_renewables.lat.isnull()].groupby(['energy_source',
                                             'data_source']
                                            )['data_source'].count()


# **Remove temporary columns**

# In[71]:

# drop lonlat column that contains both, latitute and longitude
DE_renewables.drop(['lonlat','longitude','latitude'], axis=1, inplace=True)


# ### 3.1.8 Save
#  
# The merged, translated, cleaned, DataFrame will be saved temporily as a pickle file, which stores a Python object fast.

# In[72]:

DE_renewables.to_pickle('DE_renewables.pickle')


# ## 3.2 Denmark DK

# ### 3.2.1 Download and read
# The data which will be processed below is provided by the following data sources:
# 
# ** [Energistyrelsen (ens) / Danish Energy Agency](http://www.ens.dk/info/tal-kort/statistik-noegletal/oversigt-energisektoren/stamdataregister-vindmoller)** - The wind turbines register is released by the Danish Energy Agency. 
# 
# ** [Energinet.dk](http://www.energinet.dk/DA/El/Engrosmarked/Udtraek-af-markedsdata/Sider/Statistik.aspx)** - The data of solar power plants are released by the leading transmission network operator Denmark.

# In[73]:

# point URLs to original data depending on the chosen download option
if download_from == 'original_sources':
    
    url_DK_ens = 'https://ens.dk/sites/ens.dk/files/Statistik/anlaegprodtilnettet_0.xls'
    url_DK_energinet = 'http://www.energinet.dk/SiteCollectionDocuments/Danske%20dokumenter/El/SolcelleGraf.xlsx'
    url_DK_geo = 'http://download.geonames.org/export/zip/DK.zip'

elif download_from == 'opsd_server':
    
    url_DK_ens = (url_opsd + version + folder + '/DK/anlaegprodtilnettet.xls')
    url_DK_energinet = (url_opsd + version + folder + '/DK/SolcelleGraf.xlsx')
    url_DK_geo = (url_opsd + version + folder + 'DK/DK.zip')


# In[74]:

# Get wind turbines data 
DK_wind_df = pd.read_excel(download_and_cache(url_DK_ens),
                           sheetname='IkkeAfmeldte-Existing turbines',
                           thousands='.', 
                           header=17,
                           skipfooter=3,
                           parse_cols=16,
                           converters={'Møllenummer (GSRN)': str,
                                       'Kommune-nr': str,
                                       'Postnr': str}
                          )
                         
# Get photovoltaic data
DK_solar_df = pd.read_excel(download_and_cache(url_DK_energinet),
                            sheetname='Data',
                            converters={'Postnr': str}
                           )


# In[75]:

DK_wind_df.head(2)


# In[76]:

DK_solar_df.head(2)


# ### 3.2.2 Translate column names

# In[77]:

# Choose the translation terms for Denmark, create dictionary and show dictionary
idx_DK = columnnames[columnnames['country'] == 'DK'].index
column_dict_DK = columnnames.loc[idx_DK].set_index('original_name')['opsd_name'].to_dict()
column_dict_DK


# In[78]:

# Translate columns by list 
DK_wind_df.rename(columns = column_dict_DK, inplace=True)
DK_solar_df.rename(columns = column_dict_DK, inplace=True)


# ### 3.2.3 Add data source and missing information

# In[79]:

# Add names of the data sources to the DataFrames
DK_wind_df['data_source'] = 'Energistyrelsen'
DK_solar_df['data_source'] = 'Energinet.dk'

# Add energy_source for each of the two DataFrames
DK_wind_df['energy_source'] = 'Wind'
DK_solar_df['energy_source'] = 'Solar'
DK_solar_df['energy_source_subtype'] = 'Photovoltaics'


# ### 3.2.4 Translate values and harmonize energy source

# In[80]:

idx_DK = valuenames[valuenames['country'] == 'DK'].index
value_dict_DK = valuenames.loc[idx_DK].set_index('original_name')['opsd_name'].to_dict()
value_dict_DK


# In[81]:

DK_wind_df.replace(value_dict_DK, inplace=True)


# ### 3.2.5 Georeferencing

# **UTM32 to lat/lon** *(Data from Energistyrelsen)*
# 
# The Energistyrelsen data set offers UTM Geoinformation with the columns utm_east and utm_north belonging to the UTM zone 32. In this section the existing geoinformation (in UTM-format) will be transformed into latidude and longitude coordiates as a uniform standard for geoinformation.

# In[82]:

# Index for all values with utm information
idx_notnull= DK_wind_df['utm_east'].notnull()


# In[83]:

# Convert from UTM values to latitude and longitude coordinates
DK_wind_df['lonlat'] = DK_wind_df.loc[idx_notnull,['utm_east','utm_north']
                                           ].apply(lambda x: utm.to_latlon(x[0],
                                           x[1],32,'U'), axis=1).astype(str)


# In[84]:

# Split latitude and longitude in two columns
lat = []
lon = []

for row in DK_wind_df['lonlat']:
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
        
DK_wind_df['lat'] = pd.to_numeric(lat)
DK_wind_df['lon'] = pd.to_numeric(lon)

# drop lonlat column that contains both, latitute and longitude
DK_wind_df.drop('lonlat', axis=1, inplace=True)


# **Postcode to lat/lon (WGS84)**
# *(for data from Energinet.dk)*
# 
# The available post code in the original data provides an approximation for the geocoordinates of the solar power plants.<br>
# The postcode will be assigned to latitude and longitude coordinates with the help of the postcode table.
# 
# ** [geonames.org](http://download.geonames.org/export/zip/?C=N;O=D)** The postcode  data from Denmark is provided by Geonames and licensed under a [Creative Commons Attribution 3.0 license](http://creativecommons.org/licenses/by/3.0/).

# In[85]:

# Get geo-information
zip_DK_geo = zipfile.ZipFile(download_and_cache(url_DK_geo))

# Read generated postcode/location file
DK_geo = pd.read_csv(zip_DK_geo.open('DK.txt'), sep='\t', header=-1)

# add column names as defined in associated readme file
DK_geo.columns =  ['country_code','postcode','place_name','admin_name1',
                   'admin_code1','admin_name2','admin_code2','admin_name3',
                   'admin_code3','lat','lon','accuracy']

# Drop rows of possible duplicate postal_code
DK_geo.drop_duplicates('postcode', keep='last',inplace=True)
DK_geo['postcode'] = DK_geo['postcode'].astype(str)


# In[86]:

# Add longitude/latitude infomation assigned by postcode (for Energinet.dk data)
DK_solar_df = DK_solar_df.merge(DK_geo[['postcode','lon','lat']], 
                                on=['postcode'],
                                how='left')


# In[87]:

print('Missing Coordinates DK_wind ',DK_wind_df.lat.isnull().sum())
print('Missing Coordinates DK_solar ',DK_solar_df.lat.isnull().sum())


# ### 3.2.6 Merge DataFrames and choose columns

# In[88]:

dataframes = [DK_wind_df, DK_solar_df]
DK_renewables = pd.concat(dataframes)
DK_renewables = DK_renewables.reset_index()


# In[89]:

# Only these columns will be kept for the renewable power plant list output
column_interest = ['commissioning_date', 'energy_source','energy_source_subtype',
                   'electrical_capacity_kW', 'dso','gsrn_id', 'postcode',
                   'municipality_code','municipality','address', 'address_number',
                   'utm_east', 'utm_north', 'lon','lat','hub_height',
                   'rotor_diameter', 'manufacturer', 'model', 'data_source']


# In[90]:

# Clean DataFrame from columns other than specified above
DK_renewables = DK_renewables.loc[:, column_interest]
DK_renewables.reset_index(drop=True, inplace=True)


# ### 3.2.7 Transform electrical_capacity from kW to MW

# In[91]:

# kW to MW
DK_renewables['electrical_capacity_kW'] /= 1000

# adapt column name
DK_renewables.rename(columns={'electrical_capacity_kW': 'electrical_capacity'},
                inplace=True)


# In[92]:

DK_renewables.head(2)


# ### 3.2.8 Save

# In[93]:

DK_renewables.to_pickle('DK_renewables.pickle')


# ## 3.3 France FR

# ### 3.3.1 Download and read
# The data which will be processed below is provided by the following data source:
# 
# ** [Ministery of the Environment, Energy and the Sea](http://www.statistiques.developpement-durable.gouv.fr/energie-climat/r/energies-renouvelables.html?tx_ttnews%5Btt_news%5D=24638&cHash=d237bf9985fdca39d7d8c5dc84fb95f9)** - Number of installations and installed capacity of the different renewable source for every municipality in France. Service of observation and statistics, survey, date of last update: 15/12/2015. Data until 31/12/2014.

# In[94]:

# point URLs to original data depending on the chosen download option
if download_from == 'original_sources':
    
    url_FR_gouv = "http://www.statistiques.developpement-durable.gouv.fr/fileadmin/documents/Themes/Energies_et_climat/Les_differentes_energies/Energies_renouvelables/donnees_locales/2014/electricite-renouvelable-par-commune-2014.xls"
    url_FR_geo = 'http://public.opendatasoft.com/explore/dataset/code-postal-code-insee-2015/download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true'

else:
    url_FR_gouv = (url_opsd + version + folder + '/FR/electricite-renouvelable-par-commune-2014.xls')
    url_FR_geo = (url_opsd + version + folder + 'FR/code-postal-code-insee-2015.csv')


# In[95]:

# Get data of renewables per municipality
FR_re_df = pd.read_excel(download_and_cache(url_FR_gouv),
                         sheetname='Commune', 
                         encoding = 'UTF8',  
                         thousands='.',
                         decimals=',',
                         header=[2, 3],
                         skipfooter=9,  # contains summarized values
                         index_col=[0, 1], # required for MultiIndex
                         converters={'Code officiel géographique':str})


# ### 3.3.2 Rearrange columns and translate column names

# The French data source contains number of installations and sum of installed capacity per energy source per municipality. The structure is adapted to the power plant list of other countries. The list is limited to the plants which are covered by article 10 of february 2000 by an agreement to a purchase commitment.

# In[96]:

# Rearrange data 
FR_re_df.index.rename(['insee_com', 'municipality'], inplace=True)
FR_re_df.columns.rename(['energy_source', None], inplace=True)
FR_re_df = (FR_re_df
            .stack(level='energy_source', dropna=False)
            .reset_index(drop = False))


# In[97]:

# Choose the translation terms for France, create dictionary and show dictionary
idx_FR = columnnames[columnnames['country'] == 'FR'].index
column_dict_FR = columnnames.loc[idx_FR].set_index('original_name')['opsd_name'].to_dict()
column_dict_FR


# In[98]:

# Translate columnnames
FR_re_df.rename(columns = column_dict_FR, inplace=True)


# In[99]:

# Drop all rows that just contain NA
FR_re_df = FR_re_df.dropna()
FR_re_df.head()


# ### 3.3.3 Add data source

# In[100]:

FR_re_df['data_source'] = 'gouv.fr'


# In[101]:

FR_re_df.info()


# ### 3.3.4 Translate values and harmonize energy source

# ** Kept secret if number of installations < 3**
# 
# If the number of installations is less than 3, it is marked with an _s_ instead of the number 1 or 2 due to statistical confidentiality ([further explanation by the data provider](http://www.statistiques.developpement-durable.gouv.fr/fileadmin/documents/Themes/Energies_et_climat/Les_differentes_energies/Energies_renouvelables/donnees_locales/2014/methodo-donnees-locales-electricte-renouvelable-12-2015-b.pdf)). Here, the _s_ is changed to _< 3_. This is done in the same step as the other value translations of the energy sources.

# In[102]:

idx_FR = valuenames[valuenames['country'] == 'FR'].index
value_dict_FR = valuenames.loc[idx_FR].set_index('original_name')['opsd_name'].to_dict()
value_dict_FR


# In[103]:

FR_re_df.replace(value_dict_FR, inplace=True)


# **Separate and assign energy source and subtypes**

# In[104]:

# Create dictionnary in order to assign energy_source to its subtype
energy_source_dict_FR = valuenames.loc[idx_FR].set_index('opsd_name')['energy_source'].to_dict()

# Column energy_source partly contains subtype information, thus this column is copied
# to new column for energy_source_subtype...
FR_re_df['energy_source_subtype'] = FR_re_df['energy_source']

# ...and the energy source subtype values in the energy_source column are replaced by 
# the higher level classification
FR_re_df['energy_source'].replace(energy_source_dict_FR, inplace=True)


# In[105]:

FR_re_df.reset_index(drop=True, inplace=True)


# ### 3.3.5 Georeferencing

# #### Municipality (INSEE) code to lon/lat
# The available INSEE code in the original data provides a first approximation for the geocoordinates of the renewable power plants. The following data source is utilized for assigning INSEE code to coordinates of the municipalities:
# 
# ** [OpenDataSoft](http://public.opendatasoft.com/explore/dataset/code-postal-code-insee-2015/information/)** publishes a list of French INSEE codes and corresponding coordinates is published under the [Licence Ouverte (Etalab)](https://www.etalab.gouv.fr/licence-ouverte-open-licence).

# In[106]:

# Downlad French geo-information. As download_and_cache_function is not working
# properly yet, thus other way of downloading
filename = 'code-postal-insee-2015.csv'
filepath = "input/original_data/" + filename
if not os.path.exists(filepath):
        print("Downloading file: ", filename)
        FR_geo_csv = urllib.request.urlretrieve(url_FR_geo, filepath)
else:
        print("Using local file from", filepath)


# In[107]:

# Read INSEE Code Data
FR_geo = pd.read_csv('input/original_data/code-postal-insee-2015.csv',
                     sep=';',
                     header=0,
                     converters={'Code_postal':str})

# Drop possible duplicates of the same INSEE code
FR_geo.drop_duplicates('INSEE_COM', keep='last',inplace=True)


# In[108]:

# create columns for latitude/longitude
lat = []
lon = []

# split in latitude/longitude
for row in FR_geo['Geo Point']:
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
        
# add these columns to the INSEE DataFrame
FR_geo['lat'] = pd.to_numeric(lat)
FR_geo['lon'] = pd.to_numeric(lon)


# In[109]:

# Column names of merge key have to be named identically
FR_re_df.rename(columns={'municipality_code': 'INSEE_COM'}, inplace=True)

# Merge longitude and latitude columns by the Code INSEE
FR_re_df = FR_re_df.merge(FR_geo[['INSEE_COM','lat','lon']],
                          on=['INSEE_COM'],
                          how='left')

# Translate Code INSEE column back to municipality_code
FR_re_df.rename(columns={'INSEE_COM': 'municipality_code'}, inplace=True)


# In[110]:

FR_re_df.head(2)


# ### 3.3.6 Save

# In[111]:

FR_re_df.to_pickle('FR_renewables.pickle')


# ## 3.4 Poland PL

# ### 3.4.1 Download and read
# The data which will be processed below is provided by the following data source:
# 
# ** [Urzad Regulacji Energetyki (URE) / Energy Regulatory Office](http://www.ure.gov.pl/uremapoze/mapa.html)** - Number of installations and installed capacity per energy source of renewable energy. Summed per powiat (districts) .

# #### The Polish data has to be downloaded manually 
# if you have not chosen download_from = opsd_server.
# - Go to http://www.ure.gov.pl/uremapoze/mapa.html
# - Click on the British flag in the lower right corner for Englisch version
# - Set detail to highest (to the right) in the upper right corner
# - Click on the printer symbol in the lower left corner
# - 'Generate', then the rtf-file simple.rtf will be downloaded
# - Put it in the folder input/original_data on your computer

# In[114]:

if download_from == 'opsd_server':
    url_PL_ure = (url_opsd + version + folder + '/PL/simple.rtf')
    download_and_cache(url_PL_ure)


# In[115]:

# read rtf-file to string with the correct encoding
with open('input/original_data/simple.rtf', 'r') as rtf:
    file_content = rtf.read()

file_content = file_content.encode('utf-8').decode('iso-8859-2')


# ### 3.4.2 Rearrange data from rft-file

# The rtf file has one table for each district in the rtf-file which needs to be separated from each and other and restructured to get all plants in one DataFrame with the information: district, energy_source, number_of_installations, installed_capacity. Thus in the following, the separating items are defined, the district tables split in parts, all put in one list and afterwards transferred to a pandas DataFrame.

# In[116]:

# a new line is separating all parts
sep_split_into_parts = r'{\fs12 \f1 \line }'
# separates the table rows of each table
sep_data_parts = r'\trql'

reg_exp_district = r'(?<=Powiat:).*(?=})'

reg_exp_installation_type = (
    r'(?<=\\fs12 \\f1 \\pard \\intbl \\ql \\cbpat[2|3|4] \{\\fs12 \\f1  ).*(?=\})')
reg_exp_installation_value = (
    r'(?<=\\fs12 \\f1 \\pard \\intbl \\qr \\cbpat[3|4] \{\\fs12 \\f1 ).*(?=})')

# split file into parts
parts = file_content.split(sep_split_into_parts)


# In[117]:

# list containing the data
data_set = []
for part in parts:
    # match district
    district = re.findall(reg_exp_district, part)
    if len(district) == 0:
        pass
    else:
        district = district[0].lstrip()
        # separate each part
        data_parts = part.split(sep_data_parts)
        # data structure: data_row = {'district': '', 'install_type': '', 'quantity': '', 'power': ''}
        for data_rows in data_parts:
            wrapper_list = []
            # match each installation type
            installation_type = re.findall(reg_exp_installation_type, data_rows)
            for inst_type in installation_type:
                wrapper_list.append({'district': district, 'energy_source_subtype': inst_type})
            # match data - contains twice as many entries as installation type (quantity, power vs. install type)
            data_values = re.findall(reg_exp_installation_value, data_rows)
            if len(data_values) == 0:
                #log.debug('data values empty')
                pass
            else:
                # connect data
                for i, _ in enumerate(wrapper_list):
                    wrapper_list[i]['number_of_installations'] = data_values[(i * 2)]
                    wrapper_list[i]['electrical_capacity'] = data_values[(i * 2) + 1]

                # prepare to write to file
                for data in wrapper_list:
                    data_set.append(data)


# In[118]:

# mapping of malformed unicode which appear in the Polish district names
polish_truncated_unicode_map = {
    r'\uc0\u322': 'ł',
    r'\uc0\u380': 'ż',
    r'\uc0\u243': 'ó',
    r'\uc0\u347': 'ś',
    r'\uc0\u324': 'ń',
    r'\uc0\u261': 'ą',
    r'\uc0\u281': 'ę',
    r'\uc0\u263': 'ć',
    r'\uc0\u321': 'Ł',
    r'\uc0\u378': 'ź',
    r'\uc0\u346': 'Ś',
    r'\uc0\u379': 'Ż'
}


# In[119]:

# changing malformed unicode
for entry in data_set:
    while r'\u' in entry['district']:
        index = entry['district'].index(r'\u')
        offset = index + 9
        to_be_replaced = entry['district'][index:offset]
        if to_be_replaced in polish_truncated_unicode_map.keys():
            # offset + 1 because there is a trailing whitespace
            entry['district'] = entry['district'].replace(entry['district'][index:offset + 1],
                                                  polish_truncated_unicode_map[to_be_replaced])
        else:
            break


# In[120]:

# Create pandas DataFrame with similar structure as the other countries
PL_re_df = pd.DataFrame(data_set)


# ### 3.4.3 Add data source

# In[121]:

PL_re_df['data_source'] = 'Urzad Regulacji Energetyki'


# ### 3.4.4 Translate values and harmonize energy source

# In[122]:

idx_PL = valuenames[valuenames['country'] == 'PL'].index
value_dict_PL = valuenames.loc[idx_PL].set_index('original_name')['opsd_name'].to_dict()
value_dict_PL


# In[123]:

PL_re_df.head()


# In[124]:

# Replace install_type descriptions with energy_source subtype
PL_re_df.energy_source_subtype.replace(value_dict_PL, inplace=True)


# **Assign energy_source_subtype to energy_source**

# In[125]:

# Create dictionnary in order to assign energy_source to its subtype
energy_source_dict_PL = valuenames.loc[idx_PL].set_index('opsd_name')['energy_source'].to_dict()

# Create new column for energy_source
PL_re_df['energy_source'] = PL_re_df.energy_source_subtype

# Fill this with the energy source instead of subtype information
PL_re_df.energy_source.replace(energy_source_dict_PL, inplace=True)


# In[126]:

energy_source_dict_PL


# ** Adjust datatype of numeric columns**

# In[127]:

# change type to numeric
PL_re_df['electrical_capacity'] = pd.to_numeric(PL_re_df['electrical_capacity'])
# Additionally commas are deleted
PL_re_df['number_of_installations'] = pd.to_numeric(
    PL_re_df['number_of_installations'].str.replace(',',''))


# **Aggregate**
# 
# For entries/rows of the same district and energy_source_subtype, electrical capacity and number of installations are aggregaated.

# In[128]:

PL_re_df = PL_re_df.groupby(['district','energy_source','energy_source_subtype'],
                            as_index = False
                            ).agg({'electrical_capacity': sum,
                                   'number_of_installations': sum,
                                   'data_source': 'first'})


# ### 3.4.5 Georeferencing - _work in progress_

# In[129]:

# ToDo: GeoReferencing
# to get GEOINFO
# NTS 4 - powiats and cities with powiat status (314 + 66 units)
# http://stat.gov.pl/en/regional-statistics/nomenclature-nts-161/
# http://forum.geonames.org/gforum/posts/list/795.page


# ### 3.4.6 Save

# In[130]:

PL_re_df.to_pickle('PL_renewables.pickle')


# Check and validation of the renewable power plants list as well as the creation of CSV/XLSX/SQLite files can be found in Part 2 of this script. It also generates a daily time series of cumulated installed capacities by energy source.
