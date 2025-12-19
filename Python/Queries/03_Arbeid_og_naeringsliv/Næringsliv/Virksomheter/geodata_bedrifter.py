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
try:
    script_name = os.path.basename(__file__)
except NameError:
    # __file__ is not defined in Jupyter interactive window
    script_name = "geodata_bedrifter.py"

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
    'nakgruppetekst1': 'Nace_1_navn',
    'naktittel1': 'Nace_5_navn',
    'nakkategori5': 'Nace_5_nr',
    'koordinat_y': 'Y (UTM33)',
    'koordinat_x': 'X (UTM33)',
    'load_date': 'load_date'
}

# Generate the fields string from the dictionary keys
fields = ",".join(custom_column_names.keys())

print(f"\nRequesting {len(custom_column_names)} fields from API...")
print(f"Fields: {fields[:200]}...")  # Show first 200 chars

# First, get the total count of records
count_params = {
    "where": "fylfylkenavn='Telemark'",
    "returnCountOnly": "true",
    "f": "json",
    "token": token
}

try:
    count_response = requests.get(url, params=count_params, timeout=10)
    count_response.raise_for_status()
    total_records = count_response.json()['count']
    print(f"Total records to fetch: {total_records:,}")
except Exception as e:
    print(f"Error getting record count: {e}")
    raise

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

        # Make the request with timeout
        try:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                response_data = response.json()
                
                # Check for API errors in the response
                if 'error' in response_data:
                    print(f"\nAPI Error: {response_data['error']}")
                    print(f"Full response: {response_data}")
                    break
                
                features = response_data.get('features', [])
                all_features.extend(features)
                pbar.update(len(features))
                
                # Store data for later use (field definitions)
                if offset == 0:
                    data = response_data
            else:
                print(f"\nError: HTTP {response.status_code}")
                print(f"Response: {response.text[:500]}")
                break
        except requests.exceptions.Timeout:
            print(f"\nTimeout error at offset {offset}. Retrying once...")
            try:
                response = requests.get(url, params=params, timeout=60)
                if response.status_code == 200:
                    data = response.json()
                    features = data.get('features', [])
                    all_features.extend(features)
                    pbar.update(len(features))
                else:
                    print(f"Retry failed with status {response.status_code}")
                    break
            except Exception as e:
                print(f"Retry failed with error: {e}")
                break
        except Exception as e:
            print(f"\nError fetching data at offset {offset}: {e}")
            break

# Check if we actually fetched data
if not all_features:
    raise ValueError("No data was fetched from the API. Check token validity and network connection.")

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
    
    # Build the final column rename mapping using only our custom names
    # Only rename columns that exist in the DataFrame
    name_to_alias = {raw_name: custom_name for raw_name, custom_name in custom_column_names.items() if raw_name in df.columns}
    
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

# Format NACE code to match standardized format: XX.XXX (e.g., "01.110")
if 'Nace_5_nr' in df.columns:
    def format_nace_five(value):
        str_val = str(value).zfill(5)
        return f"{str_val[0:2]}.{str_val[2:5]}"
    df['Nace_5_nr'] = df['Nace_5_nr'].apply(format_nace_five)

# Ensure all NACE-related columns are strings
nace_columns = [col for col in df.columns if 'Nace' in col]
for col in nace_columns:
    df[col] = df[col].astype(str)

# Replace decimal points with commas in Lon and Lat columns
df['Lon'] = df['Lon'].astype(str).str.replace('.', ',')
df['Lat'] = df['Lat'].astype(str).str.replace('.', ',')


##################### SQL Filtering for Filtered Dataset #####################

print("\n" + "="*80)
print("APPLYING SQL FILTERS FOR FILTERED DATASET")
print("="*80)

# Store original count for reporting
original_count = len(df)
original_employees = pd.to_numeric(df['Antall ansatte'], errors='coerce').sum()

# Start with all data (no employee filter)
df_filtered = df.copy()

# Apply SQL filter (firorgnr NOT IN firorgnrknytning)
print("\nApplying SQL filter (firorgnr NOT IN firorgnrknytning)")
print("(Removes parent organizations, keeping only lowest level entities)")

# Create set of parent organization numbers
parent_orgnr = set(df_filtered['firorgnrknytning'].dropna().astype(int).astype(str))
print(f"  Unique parent organization numbers: {len(parent_orgnr):,}")

# Apply filter: Keep only organizations whose firorgnr is NOT in the parent set
orgnr_set = set(df_filtered['firorgnr'].dropna().astype(str))
overlap = orgnr_set & parent_orgnr
print(f"  Organizations to be removed (parents): {len(overlap):,}")

records_before_sql = len(df_filtered)
df_filtered = df_filtered[~df_filtered['firorgnr'].astype(str).isin(parent_orgnr)].copy()

print(f"  Records before SQL filter: {records_before_sql:,}")
print(f"  Records after SQL filter: {len(df_filtered):,}")
print(f"  Records removed: {records_before_sql - len(df_filtered):,}")

# Detailed statistics after SQL filtering
print("\n" + "="*80)
print("FILTERED DATA SUMMARY")
print("="*80)

employees_filtered = pd.to_numeric(df_filtered['Antall ansatte'], errors='coerce')

# Enterprise counts by employee size
zero_or_missing = ((employees_filtered == 0) | employees_filtered.isna()).sum()
one_employee = (employees_filtered == 1).sum()
two_to_four = ((employees_filtered >= 2) & (employees_filtered <= 4)).sum()
five_or_more = (employees_filtered >= 5).sum()
one_or_more = (employees_filtered >= 1).sum()

# Employee counts
total_employees = employees_filtered.sum()
employees_in_enterprises_with_employees = employees_filtered[employees_filtered > 0].sum()
employees_in_5plus = employees_filtered[employees_filtered >= 5].sum()

print(f"\nOriginal dataset:")
print(f"  Total records: {original_count:,}")
print(f"  Total employees: {original_employees:,.0f}")

print(f"\nFiltered dataset (after SQL filtering):")
print(f"  Total enterprises: {len(df_filtered):,}")
print(f"  Records removed: {original_count - len(df_filtered):,} ({((original_count - len(df_filtered))/original_count*100):.1f}%)")

print(f"\nEnterprises by employee count:")
print(f"  Enterprises with 0 or missing employees: {zero_or_missing:,}")
print(f"  Enterprises with 1 employee: {one_employee:,}")
print(f"  Enterprises with 2-4 employees: {two_to_four:,}")
print(f"  Enterprises with 5 or more employees: {five_or_more:,}")
print(f"  Enterprises with 1 or more employees: {one_or_more:,}")

print(f"\nEmployee statistics:")
print(f"  Total employees: {total_employees:,.0f}")
print(f"  Employees in enterprises with employees: {employees_in_enterprises_with_employees:,.0f}")
print(f"  Employees in enterprises with 5+ employees: {employees_in_5plus:,.0f}")

# Rename and reorder columns for filtered output
print("\n" + "="*80)
print("PREPARING FILTERED OUTPUT")
print("="*80)

df_filtered_output = df_filtered.rename(columns={
    'Bedriftsnavn': 'Navn',
    'firorgnr': 'Org. nr.',
    'firorgnrknytning': 'Overordnet enhet'
})

print("✓ Renamed columns for filtered output:")
print("  'Bedriftsnavn' → 'Navn'")
print("  'firorgnr' → 'Org. nr.'")
print("  'firorgnrknytning' → 'Overordnet enhet'")

# Reorder columns: key columns first
key_columns = ['Navn', 'Org. nr.', 'Overordnet enhet', 'Antall ansatte']
other_columns = [col for col in df_filtered_output.columns if col not in key_columns]
new_column_order = key_columns + other_columns
df_filtered_output = df_filtered_output[new_column_order]

# Clean up 'Overordnet enhet' column - remove trailing .0
df_filtered_output['Overordnet enhet'] = pd.to_numeric(df_filtered_output['Overordnet enhet'], errors='coerce').apply(
    lambda x: str(int(x)) if pd.notna(x) else ''
)

print(f"✓ Reordered columns and cleaned 'Overordnet enhet'")


##################### Lagre til csv, sammenlikne og eventuell opplasting til Github #####################

print("\n" + "="*80)
print("SAVING FILTERED DATASET")
print("="*80)

github_folder = "Data/03_Arbeid og næringsliv/02_Næringsliv/Virksomheter"
temp_folder = os.environ.get("TEMP_FOLDER")
log_dir = os.environ.get("LOG_FOLDER", os.getcwd())

# Save filtered dataset
file_name_filtered = "geodata_sql_filtrerte_virksomheter.csv"
task_name_filtered = "Arbeid og naeringsliv - Geodata bedrifter filtrert"

# For filtered dataset, track all columns normally
is_new_data_filtered = handle_output_data(
    df_filtered_output, 
    file_name_filtered, 
    github_folder, 
    temp_folder, 
    keepcsv=True
)

task_name_filtered_safe = task_name_filtered.replace(".", "_").replace(" ", "_")
new_data_status_file_filtered = os.path.join(log_dir, f"new_data_status_{task_name_filtered_safe}.log")

with open(new_data_status_file_filtered, "w", encoding="utf-8") as log_file:
    log_file.write(f"{task_name_filtered_safe},{file_name_filtered},{'Yes' if is_new_data_filtered else 'No'}\n")

if is_new_data_filtered:
    print("  ✓ New filtered data detected and pushed to GitHub.")
else:
    print("  ✓ No new filtered data detected.")

print(f"  ✓ Status log: {new_data_status_file_filtered}")

print("\n" + "="*80)
print("PROCESSING COMPLETE")
print("="*80)
print(f"Filtered dataset saved: {file_name_filtered} ({len(df_filtered_output):,} records)")
