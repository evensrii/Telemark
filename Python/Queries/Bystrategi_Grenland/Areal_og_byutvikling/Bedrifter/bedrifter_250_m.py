"""
Download "Bedrifter på rutenett 250 m" from SSB's kartportal (kart.ssb.no)
for all available years (2013+), filter to Grenland, and upload to GitHub.

Uses the kart.ssb.no export API to:
1. Discover available yearly datasets via WFS GetCapabilities
2. Export CSV for each year with Telemark county filter (no auth required)
3. Filter to Grenland grid cells
4. Compare against GitHub and upload if new data is available

Metadata: https://kartkatalog.geonorge.no/Metadata/7dcf4a32-b150-48f6-bae6-f8a25fab300c
Export API: https://kart.ssb.no/api/core/v1/export/file
"""

import requests
import time
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from io import BytesIO, StringIO

import pandas as pd

from Helper_scripts.github_functions import download_github_file, handle_output_data, GITHUB_TOKEN

##################### Configuration #####################

# kart.ssb.no export API
EXPORT_URL = "https://kart.ssb.no/api/core/v1/export/file"
STATUS_URL = "https://kart.ssb.no/api/core/v1/export/status"
DATASET_ALIAS_PREFIX = "bedrifter_250m_"

# Export parameters
EXPORT_FORMAT = "Csv"
EXPORT_SRID = 32633  # UTM33
EXPORT_LANGUAGE = "nb"
EXPORT_ATTRIBUTES = ["SSBID250M", "est_tot", "emp_tot"]

# Telemark county filter (fylke code '40')
TELEMARK_FILTER = 'S_INTERSECTS("SSBID250M/geom", lookupGeom(\'0670625b-b717-7373-8000-43bd3cfe494d\', (\'40\')))'
FILTER_SRID = 32633

# Temp folder
temp_folder = os.environ.get("TEMP_FOLDER")
OUTPUT_FOLDER = Path(temp_folder)
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

# GitHub paths
github_folder = "Data/Bystrategi_Grenland/Areal_og_byutvikling/Bedrifter"
file_name = "bedrifter_250m_grenland.csv"

# Grenland filter file on GitHub
GRENLAND_FILTER_GITHUB = "Data/Bystrategi_Grenland/Areal_og_byutvikling/geografiske_definisjoner_250m_ruter.csv"

# Column mapping from SSB export names to output names
COLUMN_MAP = {
    'SSBID250M': 'ssbid_250',
    'est_tot': 'ant_virksomheter',
    'emp_tot': 'ant_ansatte'
}

print(f"Temp folder: {OUTPUT_FOLDER}")

##################### Helper functions #####################

def discover_available_years():
    """
    Discover available 250m yearly datasets from WFS GetCapabilities.
    Parses layer names matching 'ms:bedrifter_250m_YYYY'.
    Returns sorted list of years.
    """
    WFS_URL = "https://kart.ssb.no/api/mapserver/v1/wfs/bedrifter_paa_rutenett"
    params = {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "REQUEST": "GetCapabilities"
    }
    response = requests.get(WFS_URL, params=params)
    response.raise_for_status()
    
    # Parse XML to find layer names matching bedrifter_250m_YYYY
    root = ET.fromstring(response.content)
    
    years = []
    for elem in root.iter():
        tag = elem.tag
        if tag.endswith('}Name') or tag == 'Name':
            name = elem.text
            if name and name.startswith('ms:bedrifter_250m_'):
                year_str = name.replace('ms:bedrifter_250m_', '')
                if year_str.isdigit() and int(year_str) >= 2013:
                    years.append(int(year_str))
    
    return sorted(set(years))


def start_export(year, use_filter=True):
    """
    Start an export job for a given year.
    Returns (job_id, download_url) or None if the dataset doesn't exist.
    """
    alias = f"{DATASET_ALIAS_PREFIX}{year}"
    payload = {
        "dataset": alias,
        "format": EXPORT_FORMAT,
        "attributes": EXPORT_ATTRIBUTES,
        "srid": EXPORT_SRID,
        "language": EXPORT_LANGUAGE
    }
    if use_filter:
        payload["filter"] = TELEMARK_FILTER
        payload["filterSrid"] = FILTER_SRID
    
    response = requests.post(EXPORT_URL, json=payload)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    
    data = response.json()
    return data["id"], data["url"]


def wait_for_export(job_id, timeout=120, poll_interval=3):
    """
    Poll export status until complete or timeout.
    Returns True if completed successfully.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = requests.get(f"{STATUS_URL}/{job_id}")
        response.raise_for_status()
        status_data = response.json()
        
        status = status_data.get("status", "")
        if status == "complete":
            return True
        elif status in ("failed", "cancelled"):
            print(f"  Export failed: {status_data.get('message', '')}")
            return False
        
        time.sleep(poll_interval)
    
    print(f"  Export timed out after {timeout}s")
    return False


def download_export(url):
    """
    Download the exported CSV file.
    Returns a pandas DataFrame.
    """
    response = requests.get(url)
    response.raise_for_status()
    
    # CSV is semicolon-delimited with UTF-8 BOM
    content = response.content.decode('utf-8-sig')
    df = pd.read_csv(StringIO(content), sep=';', dtype=str)
    return df


##################### Check GitHub for latest data #####################

print("Checking GitHub for existing data...")
existing_data = download_github_file(f"{github_folder}/{file_name}")

##################### Discover available years #####################

print("\nDiscovering available years from kart.ssb.no...")
available_years = discover_available_years()

if not available_years:
    print("ERROR: No yearly datasets found. Check WFS connectivity.")

elif True:  # Main workflow block

    print(f"Found {len(available_years)} yearly datasets (250m):")
    print(f"  Years: {available_years[0]} - {available_years[-1]}")

    latest_available_year = max(available_years)
    print(f"  Latest available year: {latest_available_year}")

    # Determine which years to download
    needs_update = True
    if existing_data is not None:
        github_latest_year = existing_data['År'].str[:4].astype(int).max()
        print(f"\nLatest year on GitHub: {github_latest_year}")
        
        if github_latest_year >= latest_available_year:
            print(f"\nGitHub already has data up to {github_latest_year}. No update needed.")
            needs_update = False
        else:
            # Only download years newer than what GitHub has
            years_to_download = [y for y in available_years if y > github_latest_year]
            print(f"\nNew data available! GitHub has up to {github_latest_year}, source has {latest_available_year}.")
            print(f"  Years to download: {years_to_download}")
    else:
        years_to_download = available_years
        print("\nNo existing file on GitHub. Will download all years.")

    if needs_update:

        ##################### Download Grenland filter #####################

        print("\nDownloading Grenland filter file...")
        filter_url = f"https://api.github.com/repos/evensrii/Telemark/contents/{GRENLAND_FILTER_GITHUB}?ref=main"
        filter_response = requests.get(filter_url, headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3.raw",
        })
        filter_response.raise_for_status()
        grenland_df = pd.read_csv(BytesIO(filter_response.content), sep=';', dtype={'ssbid': str})
        grenland_ids = set(grenland_df['ssbid'].astype(str).str.strip())
        print(f"  Grenland grid cells: {len(grenland_ids)}")

        ##################### Download data from export API #####################

        print(f"\nDownloading {len(years_to_download)} year(s) from kart.ssb.no export API...")
        print(f"  Filter: Telemark county (code '40')")
        print()

        new_year_dfs = []

        for year in years_to_download:
            print(f"  [{year}] Starting export...", end=" ", flush=True)
            
            result = start_export(year, use_filter=True)
            if result is None:
                print("Dataset not found, skipping.")
                continue
            
            job_id, download_url = result
            
            # Wait for export to complete
            if not wait_for_export(job_id):
                print("Failed. Trying without filter...", end=" ", flush=True)
                # Fallback: download all and filter locally
                result = start_export(year, use_filter=False)
                if result is None:
                    print("Skip.")
                    continue
                job_id, download_url = result
                if not wait_for_export(job_id):
                    print("Failed again. Skip.")
                    continue
            
            # Download the CSV
            df = download_export(download_url)
            df['År'] = f"{year}-01-01"
            print(f"{len(df)} rows.")
            new_year_dfs.append(df)
            
            time.sleep(1)  # Be polite to the server

        print(f"\nTotal new: {sum(len(df) for df in new_year_dfs)} rows across {len(new_year_dfs)} year(s).")

        ##################### Filter new data to Grenland #####################

        if not new_year_dfs:
            print("No new data downloaded.")

        else:
            # Combine newly downloaded years
            new_combined_df = pd.concat(new_year_dfs, ignore_index=True)
            print(f"\nNew downloaded data: {len(new_combined_df)} rows")

            # Ensure SSBID is string and stripped
            new_combined_df['SSBID250M'] = new_combined_df['SSBID250M'].astype(str).str.strip()

            # Filter to Grenland cells
            new_grenland = new_combined_df[new_combined_df['SSBID250M'].isin(grenland_ids)].copy()
            print(f"After Grenland filter: {len(new_grenland)} rows")

            if new_grenland.empty:
                print("\nWARNING: No matching Grenland cells found in new data!")
                print(f"  Sample downloaded SSBID values: {new_combined_df['SSBID250M'].head(5).tolist()}")
                print(f"  Sample filter ssbid values: {list(grenland_ids)[:5]}")

            else:
                ##################### Format and merge with existing data #####################

                # Rename columns in new data
                rename_dict = {k: v for k, v in COLUMN_MAP.items() if k in new_grenland.columns}
                new_grenland = new_grenland.rename(columns=rename_dict)

                # Final column order
                FINAL_COLUMNS = ['ssbid_250', 'År', 'ant_virksomheter', 'ant_ansatte']
                new_grenland = new_grenland[[c for c in FINAL_COLUMNS if c in new_grenland.columns]]

                # Convert numeric columns (empty strings -> 0)
                numeric_cols = ['ant_virksomheter', 'ant_ansatte']
                for col in numeric_cols:
                    if col in new_grenland.columns:
                        new_grenland[col] = pd.to_numeric(new_grenland[col], errors='coerce').fillna(0).astype(int)

                # Merge with existing GitHub data (if any)
                if existing_data is not None:
                    print(f"\nAppending {len(new_grenland)} new rows to {len(existing_data)} existing rows...")
                    grenland_output = pd.concat([existing_data, new_grenland], ignore_index=True)
                else:
                    grenland_output = new_grenland

                # Sort by year and ssbid
                grenland_output = grenland_output.sort_values(['År', 'ssbid_250']).reset_index(drop=True)

                # Save to temp folder
                output_path = OUTPUT_FOLDER / file_name
                grenland_output.to_csv(output_path, index=False)

                print(f"\nOutput saved: {output_path.name}")
                print(f"  Rows: {len(grenland_output)}")
                print(f"  Years: {grenland_output['År'].nunique()} ({grenland_output['År'].min()} to {grenland_output['År'].max()})")
                print(f"  Unique grid cells: {grenland_output['ssbid_250'].nunique()}")

                ##################### Upload to GitHub #####################

                handle_output_data(grenland_output, file_name, github_folder, temp_folder, keepcsv=True)

                print("\n" + "=" * 60)
                print("COMPLETE")
                print("=" * 60)
                print(f"  Years: {grenland_output['År'].nunique()} ({grenland_output['År'].min()[:4]}-{grenland_output['År'].max()[:4]})")
                print(f"  Total rows: {len(grenland_output)}")
                print(f"  Unique grid cells: {grenland_output['ssbid_250'].nunique()}")
