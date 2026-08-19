"""
Download "Befolkning på rutenett 250 m" from Geonorge's download API (nedlasting.geonorge.no)
for all available years (2016+).

Uses the Geonorge v3 API to:
1. Discover individual yearly datasets in the dataset series
2. Place download orders (FGDB format, EPSG:25832, fylke Telemark)
3. Download the files
4. Convert to CSV using geopandas
5. Combine with manually downloaded data (2001-2015)
6. Compare against GitHub and upload if new data is available

API docs: https://nedlasting.geonorge.no/swagger/index.html
Metadata: https://kartkatalog.geonorge.no/Metadata/0c0ad0ce-55e8-4d73-9c12-0eb0e2454acb
"""

import requests
import time
import os
import zipfile
import shutil
import re
from pathlib import Path
from io import BytesIO

import geopandas as gpd
import pandas as pd

from Helper_scripts.github_functions import download_github_file, handle_output_data, GITHUB_TOKEN

##################### Configuration #####################

# Geonorge API
BASE_URL = "https://nedlasting.geonorge.no/api/v3"
SERIES_UUID = "0c0ad0ce-55e8-4d73-9c12-0eb0e2454acb"

# Download preferences
FORMAT_NAME = "FGDB"
PROJECTION_CODE = "25832"
PROJECTION_NAME = "EUREF89 UTM sone 32, 2d"
PROJECTION_CODESPACE = "http://www.opengis.net/def/crs/EPSG/0/25832"

# Area selection (Telemark fylke)
AREA_CODE = "40"
AREA_NAME = "Telemark"
AREA_TYPE = "fylke"

# Temp folder
temp_folder = os.environ.get("TEMP_FOLDER")
OUTPUT_FOLDER = Path(temp_folder)
OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)

# GitHub paths
github_folder = "Data/Bystrategi_Grenland/Areal_og_byutvikling/Befolkning"
github_folder_geonorge = "Data/Bystrategi_Grenland/Areal_og_byutvikling/Befolkning/GeonorgeAPI"
file_name = "befolkning_250m_grenland.csv"

# Local paths
MANUAL_FOLDER = Path(r'c:\Users\eve1509\OneDrive - Telemark fylkeskommune\Github\Telemark\Data\Bystrategi_Grenland\Areal_og_byutvikling\Befolkning\Manuelt nedlastet')
GRENLAND_FILTER_GITHUB = "Data/Bystrategi_Grenland/Areal_og_byutvikling/geografiske_definisjoner_250m_ruter.csv"

# Short temp path for FGDB extraction (avoids Windows MAX_PATH limit)
TEMP_EXTRACT_ROOT = Path(r"C:\temp\geonorge")

print(f"Temp folder: {OUTPUT_FOLDER}")

##################### Helper functions #####################

def get_series_datasets(series_uuid):
    """Get all individual datasets belonging to the dataset series."""
    url = f"https://kartkatalog.geonorge.no/api/getdata/{series_uuid}"
    response = requests.get(url)
    response.raise_for_status()
    
    data = response.json()
    serie_datasets = data.get("SerieDatasets", [])
    
    datasets = []
    for ds in serie_datasets:
        title = ds.get("Title", "")
        uuid = ds.get("Uuid", "")
        year_match = re.search(r'(\d{4})$', title)
        if year_match:
            datasets.append({
                "uuid": uuid,
                "title": title,
                "year": int(year_match.group(1))
            })
    
    datasets.sort(key=lambda x: x["year"])
    return datasets


def get_area_for_year(year):
    """
    Get the correct area code/name based on the dataset year.
    Pre-packaged files use different county structures:
      - 2016-2019: "Vestfold og Telemark (gammel)" code "38"
      - 2020+: "Telemark" code "40"
    """
    if year <= 2019:
        return {"code": "38", "name": "Vestfold og Telemark (gammel)", "type": "fylke"}
    else:
        return {"code": AREA_CODE, "name": AREA_NAME, "type": AREA_TYPE}


def place_order(metadata_uuid, year):
    """Place a download order for a single dataset."""
    url = f"{BASE_URL}/order"
    area = get_area_for_year(year)
    
    order_payload = {
        "email": "",
        "downloadAsBundle": True,
        "orderLines": [
            {
                "metadataUuid": metadata_uuid,
                "areas": [area],
                "projections": [
                    {
                        "code": PROJECTION_CODE,
                        "name": PROJECTION_NAME,
                        "codespace": PROJECTION_CODESPACE
                    }
                ],
                "formats": [
                    {
                        "name": FORMAT_NAME
                    }
                ]
            }
        ]
    }
    
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=order_payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_file(url, output_path):
    """Download a file from the given URL."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"    Downloaded: {output_path.name} ({file_size_mb:.1f} MB)")
    return output_path


##################### Check GitHub for latest data #####################

print("Checking GitHub for existing data...")
existing_data = download_github_file(f"{github_folder}/{file_name}")

##################### Discover available datasets #####################

print("Discovering datasets in the series...")
datasets = get_series_datasets(SERIES_UUID)

print(f"\nFound {len(datasets)} datasets:")
for ds in datasets:
    print(f"  {ds['year']}: {ds['title']} (UUID: {ds['uuid']})")

latest_available_year = max(ds["year"] for ds in datasets)
print(f"\nLatest available year from Geonorge: {latest_available_year}")

# Determine whether an update is needed
needs_update = True
if existing_data is not None:
    github_latest_year = existing_data['År'].str[:4].astype(int).max()
    print(f"Latest year on GitHub: {github_latest_year}")

    if github_latest_year >= latest_available_year:
        print(f"\nGitHub already has data up to {github_latest_year}. No update needed.")
        needs_update = False

        # Ensure GeonorgeAPI intermediate files are on GitHub
        for geonorge_file in ["befolkning_250m_2016_and_later.csv", "befolkning_250m_2016_and_later_grenland.csv"]:
            geonorge_check = download_github_file(f"{github_folder_geonorge}/{geonorge_file}")
            if geonorge_check is None:
                local_path = OUTPUT_FOLDER / geonorge_file
                if local_path.exists():
                    sep = ';' if '2016_and_later.csv' == geonorge_file.split('_')[-1] else ','
                    # Determine separator based on file
                    if 'grenland' not in geonorge_file:
                        df_upload = pd.read_csv(local_path, sep=';', dtype=str)
                    else:
                        df_upload = pd.read_csv(local_path, dtype=str)
                    handle_output_data(df_upload, geonorge_file, github_folder_geonorge, temp_folder, keepcsv=True)
                    print(f"  Uploaded missing file: {geonorge_file}")
                else:
                    print(f"  WARNING: {geonorge_file} not found locally or on GitHub.")
            else:
                print(f"  Already on GitHub: {geonorge_file}")
    else:
        print(f"\nNew data available! GitHub has up to {github_latest_year}, Geonorge has {latest_available_year}.")
else:
    print("\nNo existing file on GitHub. Will create new dataset.")

if needs_update:

    ##################### Download datasets from Geonorge #####################

    print(f"\nPlacing orders and downloading {len(datasets)} datasets...")
    print(f"  Format: {FORMAT_NAME}")
    print(f"  Projection: EPSG:{PROJECTION_CODE} ({PROJECTION_NAME})")
    print(f"  Area: {AREA_NAME} ({AREA_TYPE} {AREA_CODE})")
    print()

    downloaded_files = []

    for ds in datasets:
        year = ds["year"]
        uuid = ds["uuid"]
        print(f"[{year}] Placing order for: {ds['title']}")

        try:
            receipt = place_order(uuid, year)
            order_uuid = receipt.get("referenceNumber", "")
            files = receipt.get("files", [])

            if not files:
                print(f"  WARNING: No files in order response. Order UUID: {order_uuid}")
                if order_uuid and receipt.get("downloadBundleUrl"):
                    bundle_url = receipt["downloadBundleUrl"]
                    output_path = OUTPUT_FOLDER / f"befolkning_250m_{year}.zip"
                    download_file(bundle_url, output_path)
                    downloaded_files.append({"year": year, "path": output_path})
                continue

            for file_info in files:
                download_url = file_info.get("downloadUrl", "")
                status = file_info.get("status", "")

                if status != "ReadyForDownload":
                    print(f"    File not ready (status: {status}), skipping")
                    continue

                if not download_url:
                    print(f"    No download URL available")
                    continue

                output_path = OUTPUT_FOLDER / f"befolkning_250m_{year}.zip"
                download_file(download_url, output_path)
                downloaded_files.append({"year": year, "path": output_path})

        except requests.exceptions.HTTPError as e:
            print(f"  ERROR: HTTP {e.response.status_code} - {e.response.text[:200]}")
        except Exception as e:
            print(f"  ERROR: {e}")

        time.sleep(2)

    print(f"\nDownloaded {len(downloaded_files)} files.")

    ##################### Extract FGDB and convert to CSV #####################

    TEMP_EXTRACT_ROOT.mkdir(parents=True, exist_ok=True)

    print("\nExtracting and converting to CSV...")

    # Scan for zip files (allows re-running independently)
    zip_files_found = sorted(OUTPUT_FOLDER.glob("befolkning_250m_*.zip"))
    downloaded_files = []
    for zp in zip_files_found:
        year_match = re.search(r'(\d{4})', zp.stem)
        if year_match:
            downloaded_files.append({"year": int(year_match.group(1)), "path": zp})

    print(f"Found {len(downloaded_files)} zip files to process.")

    csv_files = []

    for file_info in downloaded_files:
        year = file_info["year"]
        zip_path = file_info["path"]

        print(f"\n[{year}] Processing: {zip_path.name}")

        extract_folder = TEMP_EXTRACT_ROOT / f"temp_{year}"
        extract_folder.mkdir(exist_ok=True)

        try:
            with zipfile.ZipFile(zip_path, 'r') as zf:
                zf.extractall(extract_folder)
                extracted_contents = list(extract_folder.rglob("*"))
                print(f"    Extracted {len(extracted_contents)} items")

            gdb_folders = list(extract_folder.rglob("*.gdb"))
            if not gdb_folders:
                gdb_folders = [f for f in extract_folder.iterdir() if f.suffix == '.gdb']

            if not gdb_folders:
                print(f"    WARNING: No .gdb folder found.")
                continue

            gdb_path = gdb_folders[0]
            print(f"    Reading: {gdb_path.name}")

            gdf = gpd.read_file(gdb_path)
            print(f"    Shape: {gdf.shape}")

            df = pd.DataFrame(gdf.drop(columns='geometry'))
            df['year'] = year

            csv_filename = f"befolkning_250m_{year}.csv"
            csv_path = OUTPUT_FOLDER / csv_filename
            df.to_csv(csv_path, index=False, sep=';')
            csv_files.append({"year": year, "path": csv_path, "df": df})

            print(f"    Saved CSV: {csv_filename} ({len(df)} rows)")

        except Exception as e:
            print(f"    ERROR: {e}")

        finally:
            if extract_folder.exists():
                shutil.rmtree(extract_folder, ignore_errors=True)

    # Clean up
    if TEMP_EXTRACT_ROOT.exists() and not any(TEMP_EXTRACT_ROOT.iterdir()):
        TEMP_EXTRACT_ROOT.rmdir()

    for file_info in downloaded_files:
        zip_path = file_info["path"]
        if zip_path.exists():
            zip_path.unlink()
            print(f"  Deleted: {zip_path.name}")

    print(f"\nConverted {len(csv_files)} files to CSV.")

    ##################### Combine Geonorge years #####################

    if csv_files:
        print("\nCombining all Geonorge years into a single dataset...")

        all_dfs = [info["df"] for info in csv_files]
        combined_df = pd.concat(all_dfs, ignore_index=True)

        print(f"  Total rows: {len(combined_df)}")
        print(f"  Years covered: {sorted(combined_df['year'].unique())}")

        combined_path = OUTPUT_FOLDER / "befolkning_250m_2016_and_later.csv"
        combined_df.to_csv(combined_path, index=False, sep=';')
        print(f"  Saved: {combined_path.name}")

        # Delete per-year CSV files (no longer needed after combining)
        for info in csv_files:
            if info["path"].exists():
                info["path"].unlink()
                print(f"  Deleted: {info['path'].name}")

        ##################### Filter to Grenland cells #####################

        print("\nFiltering to Grenland cells...")

        # Download filter file from GitHub (semicolon-separated)
        filter_url = f"https://api.github.com/repos/evensrii/Telemark/contents/{GRENLAND_FILTER_GITHUB}?ref=main"
        filter_response = requests.get(filter_url, headers={
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3.raw",
        })
        filter_response.raise_for_status()
        grenland_df = pd.read_csv(BytesIO(filter_response.content), sep=';', dtype={'ssbid': str})
        grenland_ids = set(grenland_df['ssbid'].astype(str).str.strip())
        print(f"  Grenland grid cells: {len(grenland_ids)}")

        # Filter combined Geonorge data
        combined_path = OUTPUT_FOLDER / "befolkning_250m_2016_and_later.csv"
        combined_df = pd.read_csv(combined_path, sep=';', dtype={'ssbid250m': str})
        combined_df['ssbid250m'] = combined_df['ssbid250m'].astype(str).str.strip()

        grenland_bef = combined_df[combined_df['ssbid250m'].isin(grenland_ids)].copy()
        print(f"  Rows matching Grenland: {len(grenland_bef)}")

        grenland_bef['År'] = grenland_bef['statistikkar'].astype(str) + '-01-01'
        grenland_output = grenland_bef[['ssbid250m', 'År', 'poptot']].rename(columns={
            'ssbid250m': 'ssbid_250',
            'poptot': 'Populasjon'
        })
        grenland_output = grenland_output.sort_values(['År', 'ssbid_250']).reset_index(drop=True)

        grenland_output_path = OUTPUT_FOLDER / "befolkning_250m_2016_and_later_grenland.csv"
        grenland_output.to_csv(grenland_output_path, index=False)
        print(f"  Saved: {grenland_output_path.name} ({len(grenland_output)} rows)")

        ##################### Combine with manual data (2001-2015) #####################

        print("\nCombining with manually downloaded data (2001-2015)...")

        manual_dfs = []
        manual_pattern = re.compile(r'befolkning_250m_(\d{4})\.csv$')

        for csv_file in sorted(MANUAL_FOLDER.glob("*.csv")):
            match = manual_pattern.search(csv_file.name)
            if not match:
                continue

            year = int(match.group(1))
            df = pd.read_csv(csv_file, sep=';', dtype={'SSBID0250M': str})

            df = df.rename(columns={'SSBID0250M': 'ssbid_250', 'pop_tot': 'Populasjon'})
            df['ssbid_250'] = df['ssbid_250'].astype(str).str.strip()
            df = df[df['ssbid_250'].isin(grenland_ids)]
            df['År'] = f"{year}-01-01"
            df = df[['ssbid_250', 'År', 'Populasjon']]
            manual_dfs.append(df)
            print(f"  {year}: {len(df)} Grenland rows")

        print(f"  Processed {len(manual_dfs)} manual files.")

        # Read GeonorgeAPI data (2016+)
        geonorge_df = pd.read_csv(grenland_output_path, dtype={'ssbid_250': str})
        print(f"  Geonorge API: {len(geonorge_df)} rows ({sorted(geonorge_df['År'].unique())[0]} to {sorted(geonorge_df['År'].unique())[-1]})")

        # Combine all data
        all_dfs = manual_dfs + [geonorge_df]
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.sort_values(['År', 'ssbid_250']).reset_index(drop=True)

        FINAL_OUTPUT_PATH = OUTPUT_FOLDER / file_name
        final_df.to_csv(FINAL_OUTPUT_PATH, index=False)

        print(f"\n  Final dataset: {FINAL_OUTPUT_PATH.name}")
        print(f"  Total rows: {len(final_df)}")
        print(f"  Years: {sorted(final_df['År'].unique())[0]} to {sorted(final_df['År'].unique())[-1]} ({final_df['År'].nunique()} years)")
        print(f"  Unique grid cells: {final_df['ssbid_250'].nunique()}")

        ##################### Upload to GitHub #####################

        # Upload GeonorgeAPI intermediate files
        handle_output_data(
            pd.read_csv(OUTPUT_FOLDER / "befolkning_250m_2016_and_later.csv", sep=';', dtype=str),
            "befolkning_250m_2016_and_later.csv",
            github_folder_geonorge,
            temp_folder,
            keepcsv=True
        )

        handle_output_data(
            pd.read_csv(OUTPUT_FOLDER / "befolkning_250m_2016_and_later_grenland.csv", dtype=str),
            "befolkning_250m_2016_and_later_grenland.csv",
            github_folder_geonorge,
            temp_folder,
            keepcsv=True
        )

        # Upload final combined file
        is_new_data = handle_output_data(final_df, file_name, github_folder, temp_folder, keepcsv=True)

        ##################### Summary #####################

        print("\n" + "=" * 60)
        print("COMPLETE")
        print("=" * 60)
        print(f"  Manual data (2001-2015): {len(manual_dfs)} years")
        print(f"  Geonorge API (2016+):    {geonorge_df['År'].nunique()} years")
        print(f"  Total: {final_df['År'].nunique()} years, {len(final_df)} rows")
        print(f"  Uploaded to GitHub: {'Yes' if is_new_data else 'No (unchanged)'}")
    else:
        print("\nNo CSV files to combine.")
