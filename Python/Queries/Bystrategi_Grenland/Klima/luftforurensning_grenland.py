import os
import sys
from io import BytesIO
from datetime import datetime
import requests
import pandas as pd

# Ensure Helper_scripts is on path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'Helper_scripts'))

from Helper_scripts.github_functions import handle_output_data, GITHUB_TOKEN  # noqa: E402

GITHUB_API_BASE = "https://api.github.com"
REPO_OWNER = "evensrii"
REPO_NAME = "Telemark"
GITHUB_BRANCH = "main"
DATA_FOLDER_GH = "Data/Bystrategi_Grenland/Klima/Luftforurensing"

OUTPUT_FILE_NAME = "luftforurensing_grenland.csv"
TASK_NAME = "Bystrategi Grenland - Luftforurensning (Excel)"


def _headers():
    return {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
        "User-Agent": "luftforurensning-grenland-script"
    }


def list_excel_files_from_github(folder_path: str) -> list:
    """
    List Excel files in the GitHub folder matching pattern:
    "PM10 og NO2 døgn [YYYY] ... .xlsx"
    Returns a list of dicts with name and download_url.
    """
    url = f"{GITHUB_API_BASE}/repos/{REPO_OWNER}/{REPO_NAME}/contents/{folder_path}?ref={GITHUB_BRANCH}"
    resp = requests.get(url, headers=_headers())
    resp.raise_for_status()
    items = resp.json()

    files = []
    for it in items:
        if it.get('type') != 'file':
            continue
        name = it.get('name', '')
        lower = name.lower()
        if lower.startswith('pm10 og no2 døgn') and lower.endswith('.xlsx'):
            files.append({
                'name': name,
                'download_url': it.get('download_url')
            })
    # Sort by year extracted from name if possible
    def _year_key(x):
        import re
        m = re.search(r"(20\d{2})", x['name'])
        return int(m.group(1)) if m else -1

    files.sort(key=_year_key)
    return files


def convert_comma_decimals_to_float(df: pd.DataFrame, exclude_columns=None) -> pd.DataFrame:
    if exclude_columns is None:
        exclude_columns = []
    df_converted = df.copy()
    for col in df.columns:
        if col in exclude_columns:
            continue
        if any(keyword in col for keyword in ['PM10', 'NO2']) and 'µg/m³' in col:
            if df[col].dtype == 'object':
                df_converted[col] = df[col].astype(str).str.replace(',', '.', regex=False)
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce').astype('float64')
    return df_converted


def load_and_combine_luftforurensing_from_github_excel() -> pd.DataFrame:
    print("=== Listing Excel files from GitHub ===")
    files = list_excel_files_from_github(DATA_FOLDER_GH)
    if not files:
        print("No Excel files found with expected pattern in GitHub folder.")
        return pd.DataFrame()

    print(f"Found {len(files)} Excel files:")
    for f in files:
        print(f"  - {f['name']}")

    combined = []
    for f in files:
        try:
            print(f"\nDownloading: {f['name']}")
            r = requests.get(f['download_url'], headers=_headers())
            r.raise_for_status()
            bio = BytesIO(r.content)
            df = pd.read_excel(bio)
            print(f"  Loaded {len(df)} rows, {len(df.columns)} cols")

            # Prepare numeric conversions
            preserve_cols = ['Fra-tid', 'Til-tid']
            preserve_cols.extend([c for c in df.columns if 'Datadekning (%)' in c])
            df = convert_comma_decimals_to_float(df, exclude_columns=preserve_cols)
            combined.append(df)
        except Exception as e:
            print(f"  Error processing {f['name']}: {e}")
            continue

    if not combined:
        print("No data could be loaded from Excel files.")
        return pd.DataFrame()

    print(f"\nCombining {len(combined)} DataFrames...")
    df_all = pd.concat(combined, ignore_index=True)

    # Sort by Fra-tid assuming format 'DD.MM.YYYY HH:MM'
    if 'Fra-tid' in df_all.columns:
        df_all['Fra-tid_datetime'] = pd.to_datetime(df_all['Fra-tid'], format='%d.%m.%Y %H:%M', errors='coerce')
        df_all = df_all.sort_values('Fra-tid_datetime').drop(columns=['Fra-tid_datetime'])
        df_all = df_all.reset_index(drop=True)

    print("\n=== Final transformations ===")
    # 1) Drop Til-tid
    if 'Til-tid' in df_all.columns:
        df_all = df_all.drop(columns=['Til-tid'])
        print("✓ Removed 'Til-tid'")

    # 2) Fra-tid -> Dato (DD.MM.YYYY)
    if 'Fra-tid' in df_all.columns:
        df_all['Dato'] = df_all['Fra-tid'].astype(str).str.split(' ').str[0]
        df_all = df_all.drop(columns=['Fra-tid'])
        # Move Dato first
        cols = ['Dato'] + [c for c in df_all.columns if c != 'Dato']
        df_all = df_all[cols]
        print("✓ Converted 'Fra-tid' to 'Dato'")

    # 3) Datadekning columns divide by 100 and cast to float64
    dek_cols = [c for c in df_all.columns if 'Datadekning' in c]
    for c in dek_cols:
        df_all[c] = pd.to_numeric(df_all[c], errors='coerce') / 100
        df_all[c] = df_all[c].astype('float64')
    if dek_cols:
        print(f"✓ Converted {len(dek_cols)} 'Datadekning' columns to fraction")

    # 4) Ensure measurement columns float64
    meas_cols = []
    for c in df_all.columns:
        if any(k in c for k in ['PM10', 'NO2']) and 'µg/m³' in c:
            meas_cols.append(c)
            if df_all[c].dtype != 'float64':
                df_all[c] = pd.to_numeric(df_all[c], errors='coerce').astype('float64')
    if meas_cols:
        print(f"✓ Ensured {len(meas_cols)} measurement columns are float64")

    print(f"Final shape: {df_all.shape}")
    return df_all


def main():
    print("=== Luftforurensning Grenland (Excel) ===")
    print(f"Script started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    df = load_and_combine_luftforurensing_from_github_excel()
    if df.empty:
        print("No data to process. Exiting.")
        return

    print(f"\nProcessed total rows: {len(df)}")
    print("Preview:")
    print(df.head(3))

    github_folder = DATA_FOLDER_GH
    temp_folder = os.environ.get("TEMP_FOLDER")

    is_new = handle_output_data(df, OUTPUT_FILE_NAME, github_folder, temp_folder, keepcsv=True)

    # Write status log
    log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
    task_name_safe = TASK_NAME.replace('.', '_').replace(' ', '_')
    status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")
    with open(status_file, 'w', encoding='utf-8') as f:
        f.write(f"{task_name_safe},multiple_files,{'Yes' if is_new else 'No'}\n")
    print(f"Status written to {status_file}")


if __name__ == "__main__":
    main()
