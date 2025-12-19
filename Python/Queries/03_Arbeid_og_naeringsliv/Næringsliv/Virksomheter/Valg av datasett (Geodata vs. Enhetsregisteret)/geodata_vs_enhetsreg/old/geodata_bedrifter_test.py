# %%
"""
Raw API request to Geodata Bedrifter API
Minimal processing - only filters by Telemark and sorts by objectid DESC
"""

import requests
import pandas as pd
import os
from tqdm import tqdm

# %%
# Define the feature service endpoint
url = "https://services.geodataonline.no/arcgis/rest/services/Geomap_UTM33_EUREF89/GeomapBedrifter/FeatureServer/0/query"
token = "alm9T3FRLOXU7oEELqWZyp8uDqK8Tr_lPYeEssbtcFg."

# Request ALL fields
fields = "*"

# %%
# First, get the total count of records
count_params = {
    "where": "fylfylkenavn='Telemark'",
    "returnCountOnly": "true",
    "f": "json",
    "token": token
}

count_response = requests.get(url, params=count_params)
total_records = count_response.json()['count']
print(f"Total records to fetch: {total_records:,}")

# %%
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

# %%
# Process the collected features - NO transformations, keep everything raw
print(f"\nProcessing {len(all_features):,} features...")

if all_features:
    # Extract features data (attributes and geometry)
    features_data = []
    for feature in all_features:
        # Combine attributes and geometry into one dictionary
        feature_dict = feature['attributes'].copy()
        if 'geometry' in feature:
            feature_dict['geometry_x'] = feature['geometry']['x']
            feature_dict['geometry_y'] = feature['geometry']['y']
        features_data.append(feature_dict)
    
    # Create DataFrame with raw data
    df = pd.DataFrame(features_data)
    
    print(f"✓ DataFrame created with {len(df):,} rows and {len(df.columns)} columns")
    print(f"\nColumn names:")
    print(df.columns.tolist())
else:
    print("No features retrieved!")
    df = pd.DataFrame()

# %%
# Arrange columns in specified order
preferred_column_order = [
    'firfirmaid', 'stfstatusfirmaid', 'orfkode', 'firorgnr', 'firorgnrknytning', 'firfirmanavn1', 
    'firfirmanavn2', 'firpopnavn1', 'firpopnavn2', 'firgatenavn', 'firjuridisknavn', 'firgatenr', 
    'firgateoppgang', 'firgatepnr', 'gatepoststed', 'firpostboksadr', 'firpostboksnr', 
    'firpostboksnavn', 'firpostbokspnr', 'postbokspoststed', 'dmadresse', 'dmpostnr', 'dmpoststed', 
    'firtlfnr', 'firmobilnr', 'firfaxnr', 'firkommnr', 'firemail', 'firemailtype', 'firurl', 
    'firantansatt', 'firoms', 'firdriftres', 'koordinat_y', 'koordinat_x', 'koordinat_kvalitet', 
    'fyllandsdel', 'fylfylkenavn', 'knrkommnavn', 'bydbydelnr', 'bydbydelnavn', 'grunnkretsnr', 
    'grunnkretsnavn', 'konsernopporgnr', 'konsernoppnavn', 'konserntopporgnr', 'konserntoppnavn', 
    'etablertdato', 'selskapetsalder', 'tlklandkode', 'tlklandnavn', 'tlkverdensdelnr', 
    'tlkverdensdelnavn', 'nakkode1', 'naktittel1', 'nakkode2', 'naktittel2', 'nakkode3', 
    'naktittel3', 'nakkode4', 'naktittel4', 'nakkode5', 'naktittel5', 'nakkode6', 'naktittel6', 
    'nakkode7', 'naktittel7', 'nakkode8', 'naktittel8', 'nakkode9', 'naktittel9', 'nakkode10', 
    'naktittel10', 'kjedekode1', 'kjedetittel1', 'kjedekode2', 'kjedetittel2', 'kjedekode3', 
    'kjedetittel3', 'kjedekode4', 'kjedetittel4', 'kjedekode5', 'kjedetittel5', 'kjedekode6', 
    'kjedetittel6', 'kjedekode7', 'kjedetittel7', 'kjedekode8', 'kjedetittel8', 'kjedekode9', 
    'kjedetittel9', 'kjedekode10', 'kjedetittel10', 'firregtype', 'ansatt_kd', 'sektorkode', 
    'status_po', 'oms_kd', 'revi_orgnr', 'revi_navn', 'revi_rolle', 'revi_adresse', 'revi_postnr', 
    'revi_poststed', 'revi_land', 'crc', 'load_date', 'nakkategori2', 'nakkategori1', 
    'nakkategori3', 'nakkategori4', 'nakkategori5', 'firgateadresse', 'dl_navn', 'id_matrikkelen', 
    'regnskapsaar', 'adresseid', 'geo_x', 'geo_y', 'nakgruppekode1', 'nakgruppetekst1', 
    'nakgruppekode2', 'nakgruppetekst2', 'nakgruppekode3', 'nakgruppetekst3', 'nakgruppekode4', 
    'nakgruppetekst4', 'nakgruppekode5', 'nakgruppetekst5', 'nakgruppekode6', 'nakgruppetekst6', 
    'nakgruppekode7', 'nakgruppetekst7', 'nakgruppekode8', 'nakgruppetekst8', 'nakgruppekode9', 
    'nakgruppetekst9', 'nakgruppekode10', 'nakgruppetekst10', 'hovedgruppe', 'hovedgruppe_navn', 
    'grunnkrets_id', 'kommune_id', 'kommunenavn', 'fylke_id', 'fylkesnavn'
]

# Get columns that exist in the DataFrame and are in the preferred order
existing_preferred = [col for col in preferred_column_order if col in df.columns]

# Get columns that are not in the preferred order (additional columns)
additional_columns = [col for col in df.columns if col not in preferred_column_order]

# Combine: preferred columns first, then additional columns
final_column_order = existing_preferred + additional_columns

# Reorder DataFrame
df = df[final_column_order]

print(f"\n✓ Reordered columns: {len(existing_preferred)} from preferred list + {len(additional_columns)} additional")

# %%
# Remove duplicates
initial_count = len(df)
df = df.drop_duplicates()
duplicates_removed = initial_count - len(df)

print(f"✓ Removed {duplicates_removed:,} duplicate rows")
print(f"  Final dataset: {len(df):,} unique records")

# %%
# Save to CSV
output_folder = r"c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\03_Arbeid og næringsliv\02_Næringsliv\Virksomheter\geodata_vs_enhetsregisteret"
output_file = os.path.join(output_folder, "geodata_raw_new_even.csv")

# Ensure output folder exists
os.makedirs(output_folder, exist_ok=True)

# Save to CSV
df.to_csv(output_file, index=False)

print(f"\n✓ Saved {len(df):,} records to:")
print(f"  {output_file}")
print(f"\nFirst 5 rows (first 10 columns):")
print(df.iloc[:5, :10])
