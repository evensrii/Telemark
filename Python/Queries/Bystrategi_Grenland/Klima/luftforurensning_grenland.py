import os
import sys
from io import BytesIO
from datetime import datetime
import requests
import pandas as pd

# Ensure Helper_scripts is on path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'Helper_scripts'))

from Helper_scripts.github_functions import GITHUB_TOKEN  # noqa: E402



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
    url = f"https://api.github.com/repos/evensrii/Telemark/contents/{folder_path}?ref=main"
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


def download_existing_csv_from_github(github_path: str) -> pd.DataFrame:
    """
    Download existing CSV file from GitHub repository.
    Returns empty DataFrame if file doesn't exist.
    """
    url = f"https://api.github.com/repos/evensrii/Telemark/contents/{github_path}?ref=main"
    try:
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
        resp = requests.get(url, headers=headers)
        if resp.status_code == 404:
            print(f"No existing file found at {github_path}")
            return pd.DataFrame()
        resp.raise_for_status()
        
        # Get file content from API response
        file_info = resp.json()
        if 'content' not in file_info:
            print(f"No content found in API response for {github_path}")
            return pd.DataFrame()
        
        # Decode base64 content
        import base64
        content = base64.b64decode(file_info['content']).decode('utf-8')
        
        # Read CSV content
        from io import StringIO
        df = pd.read_csv(StringIO(content))
        print(f"Downloaded existing CSV: {len(df)} rows, {len(df.columns)} columns")
        return df
        
    except Exception as e:
        print(f"Error downloading existing CSV: {e}")
        return pd.DataFrame()


def compare_dataframes(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> dict:
    """
    Compare two DataFrames and return detailed comparison results.
    """
    result = {
        'has_changes': False,
        'is_new_file': False,
        'changes': []
    }
    
    # Check if existing file exists
    if existing_df.empty:
        result['is_new_file'] = True
        result['has_changes'] = True
        result['changes'].append("New file - no existing data to compare")
        return result
    
    # Compare shapes
    if new_df.shape != existing_df.shape:
        result['has_changes'] = True
        result['changes'].append(f"Shape changed: {existing_df.shape} -> {new_df.shape}")
    
    # Compare columns
    new_cols = set(new_df.columns)
    existing_cols = set(existing_df.columns)
    if new_cols != existing_cols:
        result['has_changes'] = True
        added_cols = new_cols - existing_cols
        removed_cols = existing_cols - new_cols
        if added_cols:
            result['changes'].append(f"Added columns: {list(added_cols)}")
        if removed_cols:
            result['changes'].append(f"Removed columns: {list(removed_cols)}")
    
    # If shapes and columns match, compare data
    if new_df.shape == existing_df.shape and new_cols == existing_cols:
        # Align column order
        existing_df = existing_df[new_df.columns]
        
        # Compare data row by row for first few rows to identify differences
        differences_found = 0
        max_check = min(100, len(new_df))  # Check first 100 rows for performance
        
        for idx in range(max_check):
            if idx >= len(existing_df):
                break
                
            new_row = new_df.iloc[idx]
            existing_row = existing_df.iloc[idx]
            
            # Compare each column
            for col in new_df.columns:
                new_val = new_row[col]
                existing_val = existing_row[col]
                
                # Handle NaN comparisons
                if pd.isna(new_val) and pd.isna(existing_val):
                    continue
                elif pd.isna(new_val) or pd.isna(existing_val):
                    differences_found += 1
                    if differences_found <= 3:  # Show first 3 differences
                        result['changes'].append(f"Row {idx}, {col}: {existing_val} -> {new_val}")
                    break
                # Handle numeric comparisons with tolerance
                elif isinstance(new_val, (int, float)) and isinstance(existing_val, (int, float)):
                    if abs(new_val - existing_val) > 1e-10:
                        differences_found += 1
                        if differences_found <= 3:
                            result['changes'].append(f"Row {idx}, {col}: {existing_val} -> {new_val}")
                        break
                # Handle string comparisons
                elif str(new_val).strip() != str(existing_val).strip():
                    differences_found += 1
                    if differences_found <= 3:
                        result['changes'].append(f"Row {idx}, {col}: '{existing_val}' -> '{new_val}'")
                    break
        
        if differences_found > 0:
            result['has_changes'] = True
            if differences_found > 3:
                result['changes'].append(f"... and {differences_found - 3} more differences in first {max_check} rows")
    
    return result


def upload_csv_to_github(df: pd.DataFrame, github_path: str, commit_message: str) -> bool:
    """
    Upload DataFrame as CSV to GitHub repository.
    """
    try:
        # Convert DataFrame to CSV string
        csv_content = df.to_csv(index=False, encoding='utf-8')
        
        # Check if file exists to get SHA
        url = f"https://api.github.com/repos/evensrii/Telemark/contents/{github_path}"
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
        
        sha = None
        resp = requests.get(url, headers=headers)
        if resp.status_code == 200:
            sha = resp.json().get('sha')
        
        # Prepare upload payload
        import base64
        payload = {
            "message": commit_message,
            "content": base64.b64encode(csv_content.encode('utf-8')).decode('utf-8'),
            "branch": "main"
        }
        if sha:
            payload["sha"] = sha
        
        # Upload file
        upload_resp = requests.put(url, json=payload, headers=headers)
        upload_resp.raise_for_status()
        
        print(f"Successfully uploaded {github_path}")
        return True
        
    except Exception as e:
        print(f"Error uploading to GitHub: {e}")
        return False


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
    files = list_excel_files_from_github("Data/Bystrategi_Grenland/Klima/Luftforurensing")
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
    
    # Standardize columns before concatenation to ensure consistent structure
    all_columns = set()
    for df in combined:
        all_columns.update(df.columns)
    all_columns = sorted(list(all_columns))
    
    # Ensure all DataFrames have the same columns in the same order
    standardized = []
    for i, df in enumerate(combined):
        df_std = df.copy()
        # Add missing columns with NaN
        for col in all_columns:
            if col not in df_std.columns:
                df_std[col] = pd.NA
        # Reorder columns consistently
        df_std = df_std[all_columns]
        standardized.append(df_std)
        print(f"  Standardized DataFrame {i+1}: {df_std.shape}")
    
    df_all = pd.concat(standardized, ignore_index=True)

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
        print("✓ Converted 'Fra-tid' to 'Dato'")
        
        # Reorder columns to match expected structure: Dato first, then measurement/datadekning pairs
        new_order = ['Dato']
        
        # Get all measurement columns (PM10 and NO2)
        measurement_cols = [c for c in df_all.columns if any(k in c for k in ['PM10', 'NO2']) and 'µg/m³' in c]
        datadekning_cols = [c for c in df_all.columns if 'Datadekning' in c]
        
        # Sort measurement columns by station name for consistency
        measurement_cols.sort()
        
        # Add measurement columns, then their corresponding datadekning columns
        for meas_col in measurement_cols:
            new_order.append(meas_col)
            # Find corresponding datadekning column (they should be paired)
            
        # Add any remaining datadekning columns
        for dek_col in datadekning_cols:
            if dek_col not in new_order:
                new_order.append(dek_col)
        
        # Reorder the DataFrame
        df_all = df_all[new_order]
        print(f"✓ Reordered columns: Dato + {len(measurement_cols)} measurement + {len(datadekning_cols)} datadekning")

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

    # 5) Round all numeric columns to 8 decimal places for consistent precision
    numeric_cols = []
    for c in df_all.columns:
        if df_all[c].dtype in ['float64', 'float32']:
            df_all[c] = df_all[c].round(8)
            numeric_cols.append(c)
    if numeric_cols:
        print(f"✓ Rounded {len(numeric_cols)} numeric columns to 8 decimal places")

    # 6) Standardize NaN handling for consistent CSV save/load behavior
    # Only replace NaN in object columns, keep numeric columns as float64 with NaN
    for col in df_all.columns:
        if col == 'Dato':  # Keep date as string
            continue
        elif any(k in col for k in ['PM10', 'NO2']) and 'µg/m³' in col:
            # Keep measurement columns as float64, but ensure they're properly typed
            df_all[col] = pd.to_numeric(df_all[col], errors='coerce')
        elif 'Datadekning' in col:
            # Keep datadekning columns as float64
            df_all[col] = pd.to_numeric(df_all[col], errors='coerce')
    print("✓ Ensured proper data types for CSV consistency")

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
    
    # Debug: Check for duplicates and data consistency
    if 'Dato' in df.columns:
        print(f"\nDate range: {df['Dato'].min()} to {df['Dato'].max()}")
        duplicates = df.duplicated(subset=['Dato']).sum()
        if duplicates > 0:
            print(f"WARNING: Found {duplicates} duplicate dates")
            # Remove duplicates, keeping the last occurrence
            df = df.drop_duplicates(subset=['Dato'], keep='last').reset_index(drop=True)
            print(f"After removing duplicates: {len(df)} rows")
    
    print(f"Data types: {df.dtypes.value_counts().to_dict()}")
    print(f"Column order: {list(df.columns)[:5]}...{list(df.columns)[-3:]}")
    
    # Debug: Check for any NaN handling issues
    nan_counts = df.isnull().sum()
    if nan_counts.sum() > 0:
        print(f"\nNaN counts per column: {dict(nan_counts[nan_counts > 0])}")
    else:
        print("\nNo NaN values found")

    ##################### Custom comparison and upload to GitHub #####################
    
    file_name = "luftforurensing_grenland.csv"
    task_name = "Bystrategi Grenland - Luftforurensing"
    github_folder = "Data/Bystrategi_Grenland/Klima/Luftforurensing"
    github_file_path = f"{github_folder}/{file_name}"
    
    print(f"\n=== Comparing with existing GitHub data ===")
    
    # Download existing CSV from GitHub
    existing_df = download_existing_csv_from_github(github_file_path)
    
    # Compare DataFrames
    comparison = compare_dataframes(df, existing_df)
    
    # Report comparison results
    if comparison['is_new_file']:
        print("📄 New file - will be uploaded to GitHub")
    elif comparison['has_changes']:
        print("🔄 Changes detected:")
        for change in comparison['changes']:
            print(f"  • {change}")
    else:
        print("✅ No changes detected - data is identical")
    
    # Upload if there are changes
    is_new_data = comparison['has_changes']
    if is_new_data:
        commit_msg = "Updated luftforurensing_grenland.csv with Excel data" + (
            " (new file)" if comparison['is_new_file'] else ""
        )
        upload_success = upload_csv_to_github(df, github_file_path, commit_msg)
        if upload_success:
            print(f"✅ Successfully uploaded {file_name} to GitHub")
        else:
            print(f"❌ Failed to upload {file_name} to GitHub")
    
    # Write status log
    log_dir = os.environ.get("LOG_FOLDER", os.getcwd())
    task_name_safe = task_name.replace(".", "_").replace(" ", "_")
    new_data_status_file = os.path.join(log_dir, f"new_data_status_{task_name_safe}.log")
    
    with open(new_data_status_file, "w", encoding="utf-8") as log_file:
        log_file.write(f"{task_name_safe},multiple_files,{'Yes' if is_new_data else 'No'}\n")
    
    # Final summary
    if is_new_data:
        print(f"\n🎯 RESULT: New data detected and uploaded to GitHub")
    else:
        print(f"\n🎯 RESULT: No new data - Excel files match existing CSV")
    
    print(f"Status log: {new_data_status_file}")


if __name__ == "__main__":
    main()
