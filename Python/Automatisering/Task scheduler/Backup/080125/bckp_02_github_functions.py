# github_functions.py

import requests
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
import base64
import pandas as pd
import re

from Helper_scripts.email_functions import notify_updated_data

# Track the current file being processed
_current_file = None

def set_current_file(file_name):
    """Set the current file being processed"""
    global _current_file
    _current_file = file_name

def get_current_file():
    """Get the current file being processed"""
    return _current_file


## Function to fetch the GITHUB_TOKEN (environment variable) from the token.env file
def get_github_token():
    """
    Loads the GITHUB_TOKEN from a token.env file located in the PYTHONPATH/Queries directory.

    Returns:
        str: The value of the GITHUB_TOKEN environment variable.

    Raises:
        ValueError: If PYTHONPATH is not set, token.env is not found, or GITHUB_TOKEN is not in the .env file.
    """
    # Retrieve PYTHONPATH environment variable
    pythonpath = os.environ.get("PYTHONPATH")
    if not pythonpath:
        raise ValueError("PYTHONPATH environment variable is not set.")

    # Construct the full path to the Queries/token.env
    env_file_path = os.path.join(pythonpath, "token.env")
    if not os.path.exists(env_file_path):
        raise ValueError(f"token.env file not found in: {env_file_path}")

    # Load the .env file
    load_dotenv(env_file_path)
    print(f"Loaded .env file from: {env_file_path}")

    # Get the GITHUB_TOKEN from the environment
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("GITHUB_TOKEN not found in the loaded .env file.")

    print("GITHUB_TOKEN loaded successfully.")
    return github_token


GITHUB_TOKEN = get_github_token()


## Function to download a file from GitHub
def download_github_file(file_path):
    """Download a file from GitHub."""
    url = (
        f"https://api.github.com/repos/evensrii/Telemark/contents/{file_path}?ref=main"
    )
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        # Return the content as a Pandas DataFrame
        from io import StringIO

        return pd.read_csv(StringIO(response.text))
    elif response.status_code == 404:
        print(f"File not found on GitHub: {file_path}")
        return None
    else:
        print(
            f"Failed to download file: {file_path}, Status Code: {response.status_code}"
        )
        return None


## Function to upload a file to GitHub
def upload_github_file(local_file_path, github_file_path, message="Updating data"):
    """Upload a new or updated file to GitHub."""
    url = f"https://api.github.com/repos/evensrii/Telemark/contents/{github_file_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Read the local file content
    with open(local_file_path, "r", encoding="utf-8") as file:
        local_content = file.read()

    # Check if the file exists on GitHub
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        # File exists, get its SHA and content
        github_content = base64.b64decode(response.json()["content"]).decode("utf-8")
        sha = response.json()["sha"]

        # Compare local content with GitHub content
        if local_content.strip() == github_content.strip():
            return
    elif response.status_code == 404:
        # File does not exist
        sha = None
    else:
        # Log an error if the status check fails
        print(f"Failed to check file on GitHub: {response.json()}")
        return

    # Prepare the payload
    payload = {
        "message": message,
        "content": base64.b64encode(local_content.encode("utf-8")).decode("utf-8"),
        "branch": "main",
    }
    if sha:
        payload["sha"] = sha

    # Upload or update the file on GitHub
    response = requests.put(url, json=payload, headers=headers)

    if response.status_code in [201, 200]:
        print(f"File uploaded successfully: {github_file_path}")
    else:
        print(f"Failed to upload file: {response.json()}")


## Function to compare file to GitHub
def compare_to_github(input_df, file_name, github_folder, temp_folder):
    """
    Compares a DataFrame to an existing file on GitHub, and uploads the file if changes are detected.
    Implements a hierarchical comparison:
    1. Header changes (structural or year updates)
    2. Row count changes
    3. Value changes

    Parameters:
        input_df (pd.DataFrame): The DataFrame to compare and upload.
        file_name (str): The name of the file to be saved and compared.
        github_folder (str): The folder path in the GitHub repository.
        temp_folder (str): The local temporary folder for storing files.

    Returns:
        bool: True if new data is uploaded or detected, False otherwise.
    """
    # Set the current file being processed
    set_current_file(file_name)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get existing data from GitHub
    existing_data = download_github_file(f"{github_folder}/{file_name}")
    
    if existing_data is None:
        print(f"[{timestamp}] No existing file found on GitHub. This is new data.")
        upload_github_file(
            os.path.join(temp_folder, file_name),
            f"{github_folder}/{file_name}",
            message=f"Added {file_name}"
        )
        notify_updated_data(
            file_name, 
            diff_lines=None, 
            reason="New file added to repository"
        )
        return True

    # Reset index and sort columns to ensure proper comparison
    input_df = input_df.reset_index(drop=True)
    existing_data = existing_data.reset_index(drop=True)
    
    # Sort columns to ensure same order
    input_df = input_df[sorted(input_df.columns)]
    existing_data = existing_data[sorted(existing_data.columns)]

    # Normalize date formats and string values
    for col in input_df.columns:
        # Try to convert to datetime first
        try:
            # Check if the column might contain dates
            sample = input_df[col].dropna().iloc[0] if not input_df[col].isna().all() else None
            if sample and isinstance(sample, str):
                # Look for date patterns
                date_patterns = [
                    r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                    r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'  # YYYY-MM-DD HH:MM:SS
                ]
                if any(re.search(pattern, str(sample)) for pattern in date_patterns):
                    input_df[col] = pd.to_datetime(input_df[col]).dt.strftime('%Y-%m-%d')
                    existing_data[col] = pd.to_datetime(existing_data[col]).dt.strftime('%Y-%m-%d')
                    continue
        except (ValueError, TypeError):
            pass
        
        # If not a date, normalize strings
        input_df[col] = input_df[col].fillna('').astype(str).str.strip()
        existing_data[col] = existing_data[col].fillna('').astype(str).str.strip()

    # Normalize column names to handle encoding issues
    input_df.columns = [col.encode('ascii', 'ignore').decode('ascii') for col in input_df.columns]
    existing_data.columns = [col.encode('ascii', 'ignore').decode('ascii') for col in existing_data.columns]

    ####################################
    # STEP 1: Check for Header Changes #
    ####################################

    def normalize_header(col):
        """Normalize header for comparison by removing case and whitespace"""
        return str(col).strip().lower()

    def extract_year(header):
        """Extract year from header if present"""
        import re
        year_match = re.search(r'\b20\d{2}\b', header)
        return year_match.group(0) if year_match else None

    # Check for structural changes in headers
    existing_headers = [normalize_header(col) for col in existing_data.columns]
    new_headers = [normalize_header(col) for col in input_df.columns]

    headers_without_years = [h for h in existing_headers if not extract_year(h)]
    new_headers_without_years = [h for h in new_headers if not extract_year(h)]
    
    if set(headers_without_years) != set(new_headers_without_years):
        removed_headers = set(headers_without_years) - set(new_headers_without_years)
        added_headers = set(new_headers_without_years) - set(headers_without_years)
        
        change_msg = []
        if removed_headers:
            change_msg.append(f"Removed headers: {', '.join(removed_headers)}")
        if added_headers:
            change_msg.append(f"Added headers: {', '.join(added_headers)}")
            
        print(f"[{timestamp}] Header structure changed:")
        print("\n".join(change_msg))
        return True

    # Check for year changes in headers
    year_changes = []
    for old_h, new_h in zip(existing_headers, new_headers):
        old_year = extract_year(old_h)
        new_year = extract_year(new_h)
        if old_year and new_year and old_year != new_year:
            year_changes.append(f"{old_h} -> {new_h}")

    if year_changes:
        print(f"[{timestamp}] Year changes detected in headers:")
        for change in year_changes:
            print(f"  {change}")
        return True

    ####################################
    # STEP 2: Check Row Count Changes  #
    ####################################
    
    if len(input_df) != len(existing_data):
        print(f"[{timestamp}] Row count changed: {len(existing_data)} -> {len(input_df)}")
        return True

    ####################################
    # STEP 3: Check for Value Changes  #
    ####################################

    # Quick check for any changes in full dataset
    if input_df.equals(existing_data):
        print(f"[{timestamp}] No new data to upload. Skipping GitHub update.")
        return False

    print(f"[{timestamp}] Changes detected in the dataset.")

    # For large datasets, only show detailed differences for the last 200 rows
    dataset_size = len(input_df)
    is_large_dataset = dataset_size > 200

    if is_large_dataset:
        comparison_start = max(0, dataset_size - 200)
        existing_df_subset = existing_data.iloc[comparison_start:].copy()
        new_df_subset = input_df.iloc[comparison_start:].copy()
    else:
        existing_df_subset = existing_data.copy()
        new_df_subset = input_df.copy()

    # Identify key columns dynamically
    key_columns = identify_key_columns(new_df_subset)
    
    # Force 'Kommune' and 'Label' to be key columns if they exist
    forced_keys = ['Kommune', 'Label']
    key_columns = list(set(key_columns + [col for col in forced_keys if col in new_df_subset.columns]))
    
    # All other columns are value columns
    value_columns = [col for col in new_df_subset.columns if col not in key_columns]

    # Find changes in the last 200 rows
    changes = []
    
    # Reset index to make sure we can iterate properly
    existing_df_subset = existing_df_subset.reset_index(drop=True)
    new_df_subset = new_df_subset.reset_index(drop=True)
    
    for idx, new_row in new_df_subset.iterrows():
        if idx >= len(existing_df_subset):
            break
            
        old_row = existing_df_subset.iloc[idx]
        for col in value_columns:
            old_val = str(old_row[col]).strip() if pd.notna(old_row[col]) else ''
            new_val = str(new_row[col]).strip() if pd.notna(new_row[col]) else ''
            
            if old_val != new_val:
                # Create identifier string from all key columns
                identifiers = []
                for key_col in key_columns:
                    val = new_row[key_col]
                    if pd.notna(val):
                        identifiers.append(f"{key_col}: {val}")
                
                changes.append({
                    'identifiers': ' | '.join(identifiers),
                    'column': col,
                    'old_value': old_val,
                    'new_value': new_val
                })

    # Upload to GitHub since we detected changes in the full dataset
    upload_github_file(
        os.path.join(temp_folder, file_name),
        f"{github_folder}/{file_name}",
        message=f"Updated {file_name} - New data detected"
    )

    # Report changes based on where they were found
    if changes:
        if is_large_dataset:
            print(f"\n[{timestamp}] Showing up to 5 examples of changes from the last 200 rows:")
        else:
            print(f"\n[{timestamp}] Showing up to 5 examples of changes:")

        # Show only first 5 examples from the last 200 rows
        for change in changes[:5]:
            print(f"\nRow: {change['identifiers']}")
            print(f"  {change['column']}: {change['old_value']} -> {change['new_value']}")
        
        if len(changes) > 5:
            print(f"\nTotal changes found in examined rows: {len(changes)}")
            print("(Showing first 5 changes only)")
    else:
        if is_large_dataset:
            print(f"\n[{timestamp}] No changes found in the last 200 rows, but changes exist elsewhere in the dataset.")
        else:
            print(f"\n[{timestamp}] Changes detected in the dataset.")

    # Format changes for email notification
    diff_lines = []
    if changes:
        for change in changes[:5]:
            diff_dict = {'Identifiers': change['identifiers']}
            diff_dict[f"{change['column']} (Old)"] = change['old_value']
            diff_dict[f"{change['column']} (New)"] = change['new_value']
            diff_lines.append(diff_dict)

    notify_updated_data(
        file_name,
        diff_lines,
        reason=f"Changes detected in dataset" + 
              (" (showing examples from last 200 rows)" if changes and is_large_dataset else "") +
              (" (changes exist but not in last 200 rows)" if not changes and is_large_dataset else "")
    )
    return True


def identify_key_columns(df):
    """
    Dynamically identify key columns based on their characteristics:
    1. Known identifier columns (Kommune, År, etc.)
    2. Date/time columns
    3. Label columns
    """
    key_columns = []
    
    # Known measurement terms that indicate a value column (not a key)
    measurement_terms = ['andel', 'antall', 'prosent', 'rate', 'sum', 'total', 'verdi', 'mengde']
    
    # 1. Known identifier columns (case-insensitive)
    known_identifiers = {
        'exact': ['kommune', 'kommunenummer', 'kommunenr', 'label', 'år', 'year', 'dato', 'date'],
        'contains': ['_id', '_nr', '_key']
    }
    
    for col in df.columns:
        col_lower = col.lower()
        
        # Skip if column contains measurement terms
        if any(term in col_lower for term in measurement_terms):
            continue
            
        # Exact matches (with word boundaries)
        if any(col_lower == identifier for identifier in known_identifiers['exact']):
            key_columns.append(col)
            continue
        
        # Contains patterns (for specific substrings)
        if any(pattern in col_lower for pattern in known_identifiers['contains']):
            key_columns.append(col)
            continue
        
        # Try to identify date columns by checking content
        try:
            if df[col].dtype == 'object':  # Only check string columns
                # Check if values match date patterns
                sample = df[col].dropna().iloc[0]
                if isinstance(sample, str):
                    if any(pattern in sample for pattern in ['-01-01', '/01/01']):
                        key_columns.append(col)
                        continue
        except:
            pass

    return list(set(key_columns))  # Remove any duplicates

def handle_output_data(df, file_name, github_folder, temp_folder, keepcsv=False):
    """
    Handles output data:
    1. Saves the DataFrame to the temp folder.
    2. Compares it with GitHub data.
    3. Pushes to GitHub if new data is detected.
    4. Deletes the local temp file unless 'keepcsv' is True.

    Args:
        df (pd.DataFrame): DataFrame to save and compare.
        file_name (str): Name of the output file.
        github_folder (str): GitHub folder for comparison/upload.
        temp_folder (str): Temporary folder for local storage.
        keepcsv (bool): If True, keeps the CSV file in the temp folder.

    Returns:
        bool: True if new data was detected and pushed, False otherwise.
    """
    # Ensure the temp folder exists
    os.makedirs(temp_folder, exist_ok=True)

    # Save the DataFrame to a temporary file
    temp_file_path = os.path.join(temp_folder, file_name)
    df.to_csv(temp_file_path, index=False, encoding="utf-8")
    print(f"Saved file to {temp_file_path}")

    # Compare with GitHub and push new data if applicable
    is_new_data = compare_to_github(df, file_name, github_folder, temp_folder)

    # Optionally delete the temporary file after processing
    if not keepcsv:
        try:
            os.remove(temp_file_path)
            print(f"Deleted temporary file: {temp_file_path}")
        except Exception as e:
            print(f"Error deleting temporary file: {e}")
    else:
        print(f"Keeping CSV file: {temp_file_path}")

    return is_new_data