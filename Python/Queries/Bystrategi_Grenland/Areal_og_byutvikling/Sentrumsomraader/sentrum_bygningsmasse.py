"""
Script to combine annual bygningsmasse (building stock) grid datasets from GitHub.
Downloads CSV files ending with YYYY.csv, combines them into long format,
and uploads the result back to GitHub.
"""

import pandas as pd
import requests
import sys
import os
import re
from io import BytesIO

# Import GitHub functions
from Helper_scripts.github_functions import get_github_token, upload_github_file

# Script configuration
GITHUB_FOLDER = "Data/Bystrategi_Grenland/Areal_og_byutvikling/Sentrumssoner/250x250/Bygningsmasse_250"
OUTPUT_FOLDER = "Data/Bystrategi_Grenland/Areal_og_byutvikling/Sentrumssoner/250x250"
FILTER_FILE = "Data/Bystrategi_Grenland/Areal_og_byutvikling/Sentrumssoner/250x250/grenland_250_ruter.csv"
OUTPUT_FILENAME = "bygningsmasse_grenland_250.csv"
SCRIPT_NAME = os.path.basename(__file__)

# Column mapping from input to output
COLUMN_MAPPING = {
    "ogc_fid": "ogc_fid",
    "ssbid0250m": "ssbid_250",
    "bui0all": "bygg_i_alt",
    "bui99nn": "uoppgitt",
    "bui1dwe": "bolig",
    "bui1ind": "industri_og_lagerbygning",
    "bui1off": "kontor_og_forretningsbygning",
    "bui1tra": "samferdsels_og_kommunikasjonsbygning",
    "bui1hot": "hotell_og_restaurantbygning",
    "bui1edu": "kultur_og_forskningsbygning",
    "bui1hos": "helsebygning",
    "bui1pri": "fengsel_beredskapsbygning",
    "bui2det": "enebolig",
    "bui2hou": "tomannsbolig",
    "bui2row": "rekkehus_kjedehus_andre_småhus",
    "bui2mul": "store_boligbygg",
    "bui2com": "bygning_for_bofellesskap",
    "bui2hol": "fritidsbolig",
    "bui2hut": "koie_seterhus_og_lignende",
    "bui2gar": "garasje_og_uthus_til_bolig",
    "bui2ore": "annen_boligbygning",
    "bui2ind": "industribygning",
    "bui2pow": "energiforsyningsbygning",
    "bui2war": "lagerbygning",
    "bui2agr": "fiskeri_og_landbruksbygning",
    "bui2off": "kontorbygning",
    "bui2bus": "forretningsbygning",
    "bui2con": "congres_building",
    "bui2ser": "ekspedisjonsbygning_terminal",
    "bui2tel": "telekommunikasjonsbygning",
    "bui2han": "garasje_og_hangarbygning",
    "bui2roa": "veg_og_trafikktilsynsbygning",
    "bui2hot": "hotellbygning",
    "bui2acc": "bygning_for_overnatting",
    "bui2res": "restaurantbygning",
    "bui2sch": "skolebygning",
    "bui2uni": "universitet_og_høgskolebygning",
    "bui2mus": "museums_og_biblioteksbygning",
    "bui2spo": "idrettsbygning",
    "bui2ent": "kulturhus",
    "bui2rel": "bygning_for_religiøse_aktiviteter",
    "bui2hos": "sykehus",
    "bui2nur": "sykehjem",
    "bui2hea": "primærhelsebygning",
    "bui2pri": "fengselsbygning",
    "bui2eme": "beredskapsbygning",
    "bui2mon": "monument",
    "bui2toi": "offentlig_toalett"
}

def list_github_folder_contents(folder_path, token):
    """
    List all files in a GitHub folder using the GitHub API.
    
    Args:
        folder_path (str): Path to the folder in the GitHub repository
        token (str): GitHub authentication token
        
    Returns:
        list: List of file information dictionaries
    """
    url = f"https://api.github.com/repos/evensrii/Telemark/contents/{folder_path}?ref=main"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to list folder contents: {response.status_code}")
        return []


def download_csv_from_github(file_path, token):
    """
    Download a CSV file from GitHub.
    
    Args:
        file_path (str): Path to the file in the GitHub repository
        token (str): GitHub authentication token
        
    Returns:
        pd.DataFrame: DataFrame containing the CSV data, or None if download fails
    """
    url = f"https://api.github.com/repos/evensrii/Telemark/contents/{file_path}?ref=main"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github.v3.raw"
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        # Read CSV with semicolon separator
        df = pd.read_csv(BytesIO(response.content), sep=';')
        return df
    else:
        print(f"Failed to download {file_path}: {response.status_code}")
        return None


def combine_annual_datasets():
    """
    Main function to combine annual bygningsmasse grid datasets.
    Downloads files from GitHub, combines them, and uploads the result.
    Only includes grid cells that are in the Grenland filter file.
    """
    print(f"Starting script: {SCRIPT_NAME}")
    print(f"Looking for CSV files in GitHub folder: {GITHUB_FOLDER}\n")
    
    # Get GitHub token
    github_token = get_github_token()
    
    # Load filter file with valid Grenland grid IDs
    print(f"Loading filter file: {FILTER_FILE}")
    filter_df = download_csv_from_github(FILTER_FILE, github_token)
    
    if filter_df is None:
        print("Failed to load filter file. Exiting.")
        return
    
    # Get the set of valid grid IDs (case-insensitive)
    # The filter file should have a column named 'ssbid' or similar
    filter_col = None
    for col in filter_df.columns:
        if 'ssbid' in col.lower():
            filter_col = col
            break
    
    if filter_col is None:
        print(f"Could not find ssbid column in filter file. Available columns: {list(filter_df.columns)}")
        return
    
    # Convert to string, handling floats (remove .0 suffix if present)
    valid_grid_ids = set(
        filter_df[filter_col]
        .apply(lambda x: str(int(float(x))) if pd.notna(x) else '')
        .str.strip()
    )
    valid_grid_ids.discard('')  # Remove any empty strings
    print(f"Loaded {len(valid_grid_ids)} valid Grenland grid IDs from filter file")
    print(f"Sample IDs: {list(valid_grid_ids)[:5]}\n")
    
    # List all files in the GitHub folder
    files = list_github_folder_contents(GITHUB_FOLDER, github_token)
    
    if not files:
        print("No files found in the GitHub folder!")
        return
    
    # Filter for CSV files matching the pattern *YYYY.csv
    pattern = r'(\d{4})\.csv$'
    csv_files = []
    
    for file in files:
        if file['type'] == 'file':
            match = re.search(pattern, file['name'])
            if match:
                year = match.group(1)
                csv_files.append({
                    'name': file['name'],
                    'path': file['path'],
                    'year': year
                })
                print(f"Found file: {file['name']} -> Year: {year}")
    
    if not csv_files:
        print("No matching CSV files found!")
        return
    
    # Sort by year
    csv_files.sort(key=lambda x: x['year'])
    
    print(f"\nProcessing {len(csv_files)} files...\n")
    
    # Download and combine all files
    combined_data = []
    filter_stats = []  # Track filtering statistics per year
    
    for file_info in csv_files:
        print(f"Downloading and processing: {file_info['name']}")
        
        # Download CSV from GitHub
        df = download_csv_from_github(file_info['path'], github_token)
        
        if df is None:
            print(f"  - Skipping {file_info['name']} due to download error")
            continue
        
        # Debug: Print actual columns
        print(f"  - Columns found: {list(df.columns)}")
        
        # Identify the grid ID column
        grid_col = None
        for col in df.columns:
            if 'ssbid' in col.lower():
                grid_col = col
                break
        
        if grid_col is None:
            print(f"  - Warning: Could not find SSBID column, skipping file")
            continue
        
        # Remove rows where SSBID is empty (total rows)
        # This handles the totals row without SSBID
        df = df[df[grid_col].notna() & (df[grid_col].astype(str).str.strip() != '')]
        rows_after_removing_totals = len(df)
        print(f"  - Removed total rows, {rows_after_removing_totals} data rows remaining")
        
        # Filter to only include Grenland grid IDs
        # Convert SSBIDs to string, handling floats (remove .0 suffix if present)
        df[grid_col] = df[grid_col].apply(lambda x: str(int(float(x))) if pd.notna(x) else '')
        rows_before = len(df)
        
        # Debug: Show sample IDs from data file
        sample_data_ids = df[grid_col].head(5).tolist()
        print(f"  - Sample data IDs: {sample_data_ids}")
        
        df = df[df[grid_col].isin(valid_grid_ids)]
        rows_after = len(df)
        print(f"  - Filtered from {rows_before} to {rows_after} rows (Grenland only)")
        
        # Store filtering stats
        filter_stats.append({
            'Year': file_info['year'],
            'Total rows': rows_before,
            'Grenland rows': rows_after,
            'Filtered out': rows_before - rows_after
        })
        
        if len(df) == 0:
            print(f"  - No matching grid IDs found, skipping file")
            continue
        
        # Normalize column names to lowercase for consistent mapping
        df.columns = df.columns.str.lower()
        
        # Rename columns according to mapping
        df = df.rename(columns=COLUMN_MAPPING)
        
        # Add year column as datetime (YYYY-MM-DD format, using January 1st)
        df['År'] = pd.to_datetime(file_info['year'] + '-01-01')
        
        # Select columns: ssbid_250, År, and all other mapped columns (excluding ogc_fid)
        output_columns = ['ssbid_250', 'År'] + [v for k, v in COLUMN_MAPPING.items() if k not in ['ogc_fid', 'ssbid0250m']]
        # Only include columns that exist in the dataframe
        available_columns = [col for col in output_columns if col in df.columns]
        df = df[available_columns]
        
        # Ensure correct data types
        df['ssbid_250'] = df['ssbid_250'].astype(str)
        
        # Convert building columns to integer
        for col in df.columns:
            if col not in ['ssbid_250', 'År']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
        
        combined_data.append(df)
        print(f"  - Added {len(df)} rows for year {file_info['year']}")
    
    if not combined_data:
        print("\nNo data was successfully processed!")
        return
    
    # Concatenate all dataframes
    final_df = pd.concat(combined_data, ignore_index=True)
    
    # Print filtering statistics table
    if filter_stats:
        print("\n" + "="*80)
        print("FILTERING STATISTICS BY YEAR")
        print("="*80)
        stats_df = pd.DataFrame(filter_stats)
        print(stats_df.to_string(index=False))
        print("="*80)
    
    print(f"\nCombined dataset summary:")
    print(f"Total rows: {len(final_df)}")
    print(f"Columns: {list(final_df.columns)}")
    print(f"Years covered: {sorted(final_df['År'].dt.year.unique())}")
    print(f"Data types:\n{final_df.dtypes}")
    print(f"\nFirst few rows:")
    print(final_df.head(10))
    
    # Save to final destination in local GitHub repo
    # Construct path to local GitHub folder
    output_folder = os.path.join(
        r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark',
        OUTPUT_FOLDER.replace('/', os.sep)
    )
    os.makedirs(output_folder, exist_ok=True)
    output_file_path = os.path.join(output_folder, OUTPUT_FILENAME)
    
    # Convert datetime to string format for CSV output (YYYY-MM-DD)
    final_df_output = final_df.copy()
    final_df_output['År'] = final_df_output['År'].dt.strftime('%Y-%m-%d')
    
    final_df_output.to_csv(output_file_path, index=False, encoding='utf-8')
    print(f"\nSaved file to: {output_file_path}")
    
    # Check file size
    file_size_mb = os.path.getsize(output_file_path) / (1024 * 1024)
    print(f"File size: {file_size_mb:.2f} MB")
    
    if file_size_mb > 50:
        print("\n⚠️  File is too large for GitHub API upload (>50MB)")
        print("Please commit and push manually using Git:")
        print(f"  git add \"{OUTPUT_FOLDER}/{OUTPUT_FILENAME}\"")
        print(f"  git commit -m \"Updated {OUTPUT_FILENAME} - Combined {len(csv_files)} annual datasets\"")
        print("  git push")
    else:
        # Upload to GitHub via API (only for smaller files)
        github_file_path = f"{OUTPUT_FOLDER}/{OUTPUT_FILENAME}"
        print(f"\nUploading to GitHub: {github_file_path}")
        
        upload_github_file(
            local_file_path=output_file_path,
            github_file_path=github_file_path,
            message=f"Updated {OUTPUT_FILENAME} - Combined {len(csv_files)} annual datasets"
        )
    
    print("\nScript completed successfully!")
    return final_df


if __name__ == "__main__":
    df = combine_annual_datasets()
