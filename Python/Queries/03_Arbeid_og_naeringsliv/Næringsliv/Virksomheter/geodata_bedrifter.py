import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat
from pyproj import Transformer

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder, fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

# Endepunkt for SSB API
POST_URL = "https://data.ssb.no/api/v0/no/table/10309/"
 
#Se: https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/ og https://dokumentasjon.geodataonline.no/docs/Temakart/Bedrifter/

##### Query

# Define the feature service endpoint
url = "https://services.geodataonline.no/arcgis/rest/services/Geomap_UTM33_EUREF89/GeomapBedrifter/FeatureServer/0/query"
token = "alm9T3FRLOXU7oEELqWZyp8uDqK8Tr_lPYeEssbtcFg."

# Define the fields to retrieve
fields = "objectid,stfstatusfirmaid,orfkode,firorgnr,konsernopporgnr,konsernoppnavn,firorgnrknytning,firfirmanavn1,fylfylkenavn,fylkesnavn,kommune_id,firkommnr,kommunenavn,knrkommnavn,firantansatt,ansatt_kd,firoms,firdriftres,etablertdato,sektorkode,status_po,nakkategori1,naktittel1,nakgruppetekst1,nakkategori2,nakkategori3,nakkategori4,nakkategori5,koordinat_y,koordinat_x,load_date"

# Define parameters
params = {
    "where": "fylfylkenavn='Telemark'",  # Query condition
    "outFields": fields,  # Get all fields
    "orderByFields": "firantansatt DESC",
    "returnGeometry": "true",
    "f": "json",  # Format response as JSON
    "resultRecordCount": "1",
    "defaultSR" : "25833",
    "token": token,
    #"datumTransformation": "25833",
    #"datumTransformation" : {"wkt": "25833"},
    #"datumTransformation": {"geoTransforms":[{"wkid":25833,"forward":"true"},{"wkt":"25833","forward":"true"}]},
    #"returnCountOnly": "true",
}
 
# Make the request
response = requests.get(url, params=params)
 
# Check if request was successful
if response.status_code == 200:
    data = response.json()
    
    # Create name to alias mapping
    name_to_alias = {field['name']: field['alias'] for field in data['fields']}
    
    # Extract features data (attributes and geometry)
    features_data = []
    for feature in data['features']:
        # Combine attributes and geometry into one dictionary
        feature_dict = feature['attributes'].copy()
        if 'geometry' in feature:
            feature_dict['geometry_x'] = feature['geometry']['x']
            feature_dict['geometry_y'] = feature['geometry']['y']
        features_data.append(feature_dict)
    
    # Create DataFrame
    df = pd.DataFrame(features_data)
    
    # Rename columns using the name_to_alias mapping
    # Add geometry columns to the mapping if they exist
    if 'geometry_x' in df.columns:
        name_to_alias['geometry_x'] = 'X (UTM33)'
        name_to_alias['geometry_y'] = 'Y (UTM33)'

    # Rename the columns
    df = df.rename(columns=name_to_alias)

    # Ensure that the correct column names exist
    if 'X (UTM33)' in df.columns and 'Y (UTM33)' in df.columns:
        # Convert from UTM 33 to WGS 84
        transformer_to_latlon = Transformer.from_crs("EPSG:25833", "EPSG:4326", always_xy=True)
        lat_lon = [transformer_to_latlon.transform(x, y) for x, y in zip(df['X (UTM33)'], df['Y (UTM33)'])]

        # Store correctly named Latitude/Longitude columns
        df['Longitude (WGS84)'] = [coord[0] for coord in lat_lon]
        df['Latitude (WGS84)'] = [coord[1] for coord in lat_lon]
        
        print("\nFirst few rows with coordinates:")
        print(df[['Bedriftsnavn', 'X (UTM33)', 'Y (UTM33)', 'Latitude (WGS84)', 'Longitude (WGS84)']].head())
    else:
        print(f"Error: Missing expected coordinate columns in DataFrame.")

df = df.rename(columns={"Longitude (WGS84)": "Lon", "Latitude (WGS84)": "Lat"})

# df.info()

##### Data cleaning

# Rename columns
#df = df.rename(columns={"Bedriftsnavn": "Company_name",})

# Print column names
# print(df.columns)

# Change order of columns
column_list = [
    'objectid',
    'Bedriftsnavn',
    'Organisasjonsnummer',
    'Bedriftsstatus',
    'Organisasjonsform',
    'Antall ansatte',
    'Intervallkode antall ansatte',
    'Orgnr til overliggende enhet',
    'Orgnr overliggende enhet',
    'Navn på overliggende enhet',
    'Fylkesnavn',
    'fylkesnavn',
    'kommune_id',
    'Kommunenummer',
    'kommunenavn',
    'Kommunenavn',
    'Omsetning i hele tusen',
    'Driftsresultat i hele tusen',
    'Dato for etablering',
    'Privat eller offentlig',
    'Sektorkode fra SSB',
    'Nacetittel1',
    'Bransjegruppe 1',
    'NACE-kategori (Første siffer)',
    'NACE-kategori (To første siffer)',
    'NACE-kategori (Tre første siffer)',
    'NACE-kategori (Fire første siffer)',
    'NACE-kode (numerisk)',
    'load_date',
    'Lon',
    'Lat'
]

df = df[column_list]

##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

file_name = "geodata_bedrifter_api.csv"
task_name = "Arbeid og naeringsliv - Geodata bedrifter"
github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Virksomheter"
temp_folder = os.environ.get("TEMP_FOLDER")

# Only track changes in objectid
value_columns = ['objectid']

# Ignore all other columns
ignore_patterns = [col for col in df.columns if col != 'objectid']

# Call the function and get the "New Data" status
is_new_data = handle_output_data(
    df, 
    file_name, 
    github_folder, 
    temp_folder, 
    keepcsv=True,
    value_columns=value_columns,
    ignore_patterns=ignore_patterns
)

# Write the "New Data" status to a unique log file
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())  # Default to current working directory
task_name_safe = task_name.replace(".", "_").replace(" ", "_")  # Ensure the task name is file-system safe
new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")

# Write the result in a detailed format
with open(new_data_status_file, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_safe},{file_name},{'Yes' if is_new_data else 'No'}\n")

# Output results for debugging/testing
if is_new_data:
    print("New data detected and pushed to GitHub.")
else:
    print("No new data detected.")

print(f"New data status log written to {new_data_status_file}")
