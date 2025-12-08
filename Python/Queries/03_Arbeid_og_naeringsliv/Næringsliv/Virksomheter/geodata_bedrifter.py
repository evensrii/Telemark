import requests
import sys
import os
import glob
from io import BytesIO
from io import StringIO
import pandas as pd
from pyjstat import pyjstat
from pyproj import Transformer
from tqdm import tqdm

# Import the utility functions from the Helper_scripts folder
from Helper_scripts.utility_functions import delete_files_in_temp_folder, fetch_data
from Helper_scripts.email_functions import notify_errors
from Helper_scripts.github_functions import upload_github_file, download_github_file, compare_to_github, handle_output_data

# Capture the name of the current script
script_name = os.path.basename(__file__)

# Example list of error messages to collect errors during execution
error_messages = []

#Se: https://developers.arcgis.com/rest/services-reference/enterprise/query-feature-service-layer/ og https://dokumentasjon.geodataonline.no/docs/Temakart/Bedrifter/

##### Query

# Define the feature service endpoint
url = "https://services.geodataonline.no/arcgis/rest/services/Geomap_UTM33_EUREF89/GeomapBedrifter/FeatureServer/0/query"
token = "alm9T3FRLOXU7oEELqWZyp8uDqK8Tr_lPYeEssbtcFg."

# Column name mappings: API field name → Output column name
# The KEYS define which fields to fetch from the API
# The VALUES define what to name them in the output
# The ORDER of entries dictates the column order in the final CSV

custom_column_names = {
    'firfirmaid': 'firfirmaid',
    'stfstatusfirmaid': 'Bedriftsstatus',
    'orfkode': 'Organisasjonsform',
    'firorgnr': 'firorgnr',
    'firorgnrknytning': 'firorgnrknytning',
    'konsernopporgnr': 'konsernopporgnr',
    'konsernoppnavn': 'Navn på overliggende enhet',
    'konserntopporgnr': 'Orgnr til toppenhet',
    'firfirmanavn1': 'Bedriftsnavn',
    'fylfylkenavn': 'Fylkesnavn',
    'fylkesnavn': 'fylkesnavn',
    'kommune_id': 'kommune_id',
    'firkommnr': 'Kommunenummer',
    'kommunenavn': 'kommunenavn',
    'knrkommnavn': 'Kommunenavn',
    'firantansatt': 'Antall ansatte',
    'ansatt_kd': 'Intervallkode antall ansatte',
    'firoms': 'Omsetning i hele tusen',
    'firdriftres': 'Driftsresultat i hele tusen',
    'etablertdato': 'Dato for etablering',
    'sektorkode': 'Sektorkode fra SSB',
    'status_po': 'Privat eller offentlig',
    'nakkategori1': 'NACE-kategori (Første siffer)',
    'naktittel1': 'Nacetittel1',
    'nakgruppetekst1': 'Bransjegruppe 1',
    'nakkategori2': 'NACE-kategori (To første siffer)',
    'nakkategori3': 'NACE-kategori (Tre første siffer)',
    'nakkategori4': 'NACE-kategori (Fire første siffer)',
    'nakkategori5': 'NACE-kode (numerisk)',
    'koordinat_y': 'Y (UTM33)',
    'koordinat_x': 'X (UTM33)',
    'load_date': 'load_date'
}

# Generate the fields string from the dictionary keys
fields = ",".join(custom_column_names.keys())

# First, get the total count of records
count_params = {
    "where": "fylfylkenavn='Telemark'",
    "returnCountOnly": "true",
    "f": "json",
    "token": token
}

count_response = requests.get(url, params=count_params)
total_records = count_response.json()['count']

# Define the batch size for pagination
batch_size = 1000
all_features = []

# Create progress bar
with tqdm(total=total_records, desc="Fetching records") as pbar:
    for offset in range(0, total_records, batch_size):
        # Define parameters for each batch
        params = {
            "where": "fylfylkenavn='Telemark'",
            "outFields": fields,
            "orderByFields": "firfirmaid DESC",  # Sort by firfirmaid descending
            "returnGeometry": "true",
            "f": "json",
            "resultOffset": str(offset),
            "resultRecordCount": str(batch_size),
            "defaultSR": "25833",
            "token": token,
        }

        # Make the request
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            features = data.get('features', [])
            all_features.extend(features)
            pbar.update(len(features))
        else:
            print(f"Error: {response.status_code}")
            break

# Process the collected features
if all_features:
    # Extract features data (attributes and geometry)
    features_data = []
    for feature in all_features:
        # Combine attributes and geometry into one dictionary
        feature_dict = feature['attributes'].copy()
        if 'geometry' in feature:
            feature_dict['koordinat_y'] = feature['geometry']['y']  # Note: koordinat_y is Y in UTM33
            feature_dict['koordinat_x'] = feature['geometry']['x']  # Note: koordinat_x is X in UTM33
        features_data.append(feature_dict)
    
    # Create DataFrame
    df = pd.DataFrame(features_data)
    
    # Build the final column rename mapping
    # Start with API's default aliases
    name_to_alias = {field['name']: field['alias'] for field in data['fields']}
    
    # Override with custom names from our dictionary
    for raw_name, custom_name in custom_column_names.items():
        if raw_name in df.columns:
            name_to_alias[raw_name] = custom_name
    
    # Rename the columns
    df = df.rename(columns=name_to_alias)
    
    # Reorder columns based on custom_column_names dictionary order
    # Get the renamed column names in the order they appear in the dictionary
    ordered_columns = [custom_column_names[key] for key in custom_column_names.keys() if key in name_to_alias.keys()]
    # Add any remaining columns not in the dictionary
    remaining_columns = [col for col in df.columns if col not in ordered_columns]
    final_column_order = ordered_columns + remaining_columns
    df = df[final_column_order]

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

# Print sample of dates to check format
print("\nSample of establishment dates before conversion:")
print(df['Dato for etablering'].head())

# Convert establishment date and load_date to datetime
df['Dato for etablering'] = pd.to_datetime(df['Dato for etablering'], unit='ms')
df['load_date'] = pd.to_datetime(df['load_date'], unit='ms')

# Print sample after conversion
print("\nSample of establishment dates after conversion:")
print(df['Dato for etablering'].head())
print("\nSample of load dates after conversion:")
print(df['load_date'].head())

##### Data cleaning

# Create dictionary for organization form codes
org_form_dict = {
    'AAFY': 'Underenhet til ikke-næringsdrivende',
    'ADOS': 'Administrativ enhet -offentlig sektor',
    'ANNA': 'Annen juridisk person',
    'ANS': 'Ansvarlig selskap med solidarisk ansvar',
    'AS': 'Aksjeselskap',
    'ASA': 'Allmennaksjeselskap',
    'BA': 'Selskap med begrenset ansvar',
    'BBL': 'Boligbyggelag',
    'BEDR': 'Underenhet til næringsdrivende og offentlig forvaltning',
    'BO': 'Andre bo',
    'BRL': 'Borettslag',
    'DA': 'Ansvarlig selskap med delt ansvar',
    'ENK': 'Enkeltpersonforetak',
    'EOFG': 'Europeisk økonomisk foretaksgruppe',
    'ESEK': 'Eierseksjonssameie',
    'FKF': 'Fylkeskommunalt foretak',
    'FLI': 'Forening/lag/innretning',
    'FYLK': 'Fylkeskommune',
    'GFS': 'Gjensidig forsikringsselskap',
    'IKJP': 'Andre ikke-juridiske personer',
    'IKS': 'Interkommunalt selskap',
    'KBO': 'Konkursbo',
    'KF': 'Kommunalt foretak',
    'KIRK': 'Den norske kirke',
    'KOMM': 'Kommune',
    'KS': 'Kommandittselskap',
    'KTRF': 'Kontorfellesskap',
    'NUF': 'Norskregistrert utenlandsk foretak',
    'OPMV': 'Særskilt oppdelt enhet, jf. mval. § 2-2',
    'ORGL': 'Organisasjonsledd',
    'PERS': 'Andre enkeltpersoner som registreres i tilknyttet register',
    'PK': 'Pensjonskasse',
    'PRE': 'Partrederi',
    'SA': 'Samvirkeforetak',
    'SAM': 'Tingsrettslig sameie',
    'SE': 'Europeisk selskap',
    'SF': 'Statsforetak',
    'SPA': 'Sparebank',
    'STAT': 'Staten',
    'STI': 'Stiftelse',
    'SÆR': 'Annet foretak iflg. særskilt lov',
    'TVAM': 'Tvangsregistrert for MVA',
    'UTLA': 'Utenlandsk enhet',
    'VPFO': 'Verdipapirfond'
}

# Replace organization form codes with their full names
df['Organisasjonsform'] = df['Organisasjonsform'].map(org_form_dict)

# Remove duplicates
initial_count = len(df)
df = df.drop_duplicates()
duplicates_removed = initial_count - len(df)
print(f"✓ Removed {duplicates_removed:,} duplicate rows")
print(f"  Final dataset: {len(df):,} unique records")

# In column "NACE-kategori (To første siffer)", ensure all numbers are two digits
df['NACE-kategori (To første siffer)'] = df['NACE-kategori (To første siffer)'].astype(str).str.zfill(2)

# Format "NACE-kategori (Tre første siffer)" to XX.X format
def format_nace_three(value):
    # Convert to string and pad with leading zeros to ensure 3 digits
    str_val = str(value).zfill(3)
    # Format as XX.X
    return f"{str_val[0:2]}.{str_val[2]}"

df['NACE-kategori (Tre første siffer)'] = df['NACE-kategori (Tre første siffer)'].apply(format_nace_three)

# Format "NACE-kategori (Fire første siffer)" to XX.XX format
def format_nace_four(value):
    # Convert to string and pad with leading zeros to ensure 4 digits
    str_val = str(value).zfill(4)
    # Format as XX.XX
    return f"{str_val[0:2]}.{str_val[2:4]}"

df['NACE-kategori (Fire første siffer)'] = df['NACE-kategori (Fire første siffer)'].apply(format_nace_four)

# Format "NACE-kode (numerisk)" to XX.XXX format
def format_nace_numeric(value):
    # Convert to string and pad with leading zeros to ensure 5 digits
    str_val = str(value).zfill(5)
    # Format as XX.XXX
    return f"{str_val[0:2]}.{str_val[2:5]}"

df['NACE-kode (numerisk)'] = df['NACE-kode (numerisk)'].apply(format_nace_numeric)

# Create Kode column with the five digits without period
df['Kode'] = df['NACE-kode (numerisk)'].str.replace('.', '')

# Ensure all NACE-related columns are strings
nace_columns = [
    'NACE-kategori (Første siffer)',
    'NACE-kategori (To første siffer)',
    'NACE-kategori (Tre første siffer)',
    'NACE-kategori (Fire første siffer)',
    'NACE-kode (numerisk)',
    'Kode'
]

df[nace_columns] = df[nace_columns].astype(str)

# Replace decimal points with commas in Lon and Lat columns
df['Lon'] = df['Lon'].astype(str).str.replace('.', ',')
df['Lat'] = df['Lat'].astype(str).str.replace('.', ',')


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
