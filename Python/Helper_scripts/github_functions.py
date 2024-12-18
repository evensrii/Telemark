# github_functions.py

import requests
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
import base64
import pandas as pd

from Helper_scripts.email_functions import notify_updated_data


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
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Save the DataFrame to a CSV in the Temp folder
    local_file_path = os.path.join(temp_folder, file_name)
    input_df.to_csv(local_file_path, index=False, encoding='utf-8')

    # GitHub configuration
    github_file_path = f"{github_folder}/{file_name}"

    # Download the existing file from GitHub
    existing_data = download_github_file(github_file_path)

    # If file doesn't exist on GitHub, upload it as new
    if existing_data is None:
        print(f"[{timestamp}] Uploading new file: {file_name}")
        upload_github_file(
            local_file_path, github_file_path, message=f"Added {file_name}"
        )
        notify_updated_data(
            file_name, 
            diff_lines=None, 
            reason="New file added to repository"
        )
        return True

    # STEP 1: Check header changes
    def normalize_header(col):
        """Normalize header for comparison by removing case and whitespace"""
        return str(col).strip().lower()
    
    def extract_year(header):
        """Extract year from header if present"""
        import re
        year_match = re.search(r'20\d{2}', header)
        return year_match.group(0) if year_match else None

    existing_headers = [normalize_header(col) for col in existing_data.columns]
    new_headers = [normalize_header(col) for col in input_df.columns]

    # Check for structural changes in headers (ignoring case)
    headers_without_years = [h.replace('2023', '').replace('2024', '') for h in existing_headers]
    new_headers_without_years = [h.replace('2023', '').replace('2024', '') for h in new_headers]
    
    if set(headers_without_years) != set(new_headers_without_years):
        # Find which headers changed
        removed_headers = set(headers_without_years) - set(new_headers_without_years)
        added_headers = set(new_headers_without_years) - set(headers_without_years)
        
        change_msg = []
        if removed_headers:
            change_msg.append(f"Removed headers: {', '.join(removed_headers)}")
        if added_headers:
            change_msg.append(f"Added headers: {', '.join(added_headers)}")
            
        print(f"[{timestamp}] Header structure changed in {file_name}:")
        print("\n".join(change_msg))
        
        upload_github_file(
            local_file_path,
            github_file_path,
            message=f"Updated {file_name} - Header structure changed"
        )
        notify_updated_data(
            file_name,
            diff_lines=None,
            reason=f"Header structure changed: {'; '.join(change_msg)}"
        )
        return True

    # Check for year changes in headers
    year_changes = []
    for old_h, new_h in zip(existing_headers, new_headers):
        old_year = extract_year(old_h)
        new_year = extract_year(new_h)
        if old_year and new_year and old_year != new_year:
            year_changes.append(f"{old_h} → {new_h}")

    if year_changes:
        print(f"[{timestamp}] Year changes detected in headers for {file_name}:")
        for change in year_changes:
            print(f"  {change}")
            
        upload_github_file(
            local_file_path,
            github_file_path,
            message=f"Updated {file_name} - Header years updated"
        )
        notify_updated_data(
            file_name,
            diff_lines=None,
            reason=f"Header years changed: {'; '.join(year_changes)}"
        )
        return True

    # STEP 2: Check row count changes
    # Only runs if no header changes were found
    existing_row_count = len(existing_data)
    new_row_count = len(input_df)

    if existing_row_count != new_row_count:
        print(f"[{timestamp}] Row count changed in {file_name}:")
        print(f"  Old count: {existing_row_count}")
        print(f"  New count: {new_row_count}")
        print(f"  Difference: {new_row_count - existing_row_count:+d} rows")
            
        upload_github_file(
            local_file_path,
            github_file_path,
            message=f"Updated {file_name} - Row count changed from {existing_row_count} to {new_row_count}"
        )
        notify_updated_data(
            file_name,
            diff_lines=None,
            reason=f"Row count changed: {existing_row_count} → {new_row_count} ({new_row_count - existing_row_count:+d} rows)"
        )
        return True

    # STEP 3: Check value changes
    # Only runs if both headers and row counts are identical
    # Standardize column names for comparison
    existing_df = existing_data.copy()
    new_df = input_df.copy()

    def identify_key_columns(df):
        """
        Dynamically identify key columns based on their characteristics:
        1. Known identifier columns (Kommune, År, etc.)
        2. Date/time columns
        3. Label columns
        4. Categorical columns with very low cardinality relative to row count
        """
        key_columns = []
        
        # 1. Known identifier columns (case-insensitive)
        # Make the matching more precise with word boundaries
        known_identifiers = {
            'exact': ['kommune', 'kommunenummer', 'kommunenr', 'label', 'år', 'year', 'dato', 'date'],
            'contains': ['_id', '_nr', '_key']  # Only match these patterns
        }
        
        for col in df.columns:
            col_lower = col.lower()
            
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

            # Check for categorical/low-cardinality columns
            # Only consider a column as key if it has very few unique values compared to row count
            if len(df) > 1:  # Avoid division by zero
                unique_ratio = df[col].nunique() / len(df)
                if unique_ratio <= 0.1:  # If less than 10% of values are unique
                    key_columns.append(col)
        
        return list(set(key_columns))  # Remove any duplicates

    # Identify key columns dynamically
    key_columns = identify_key_columns(new_df)
    
    # Force 'Kommune' and 'Label' to be key columns if they exist
    forced_keys = ['Kommune', 'Label']
    key_columns = list(set(key_columns + [col for col in forced_keys if col in new_df.columns]))
    
    # All other columns are value columns
    value_columns = [col for col in new_df.columns if col not in key_columns]

    print(f"[{timestamp}] Identified key columns: {', '.join(key_columns)}")
    print(f"[{timestamp}] Value columns to compare: {', '.join(value_columns)}\n")

    # Find rows where values have changed
    changes = []
    for idx, new_row in new_df.iterrows():
        old_row = existing_df.loc[idx]
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

    if changes:
        print(f"[{timestamp}] New data detected. Uploading to GitHub.")
        print(f"\n[{timestamp}] === Data Comparison ===")
        print(f"File: {file_name}")
        print("Changes detected in the following rows:\n")

        # Show up to 5 examples of changes
        for change in changes[:5]:
            print(f"Row: {change['identifiers']}")
            print(f"  {change['column']}: {change['old_value']} → {change['new_value']}")
            print()

        total_changes = len(changes)
        print(f"Total rows with changes: {total_changes}")
        if total_changes > 5:
            print("(Showing first 5 changes only)")
        print("=" * 20 + "\n")

        upload_github_file(
            local_file_path,
            github_file_path,
            message=f"Updated {file_name} with {total_changes} value changes"
        )

        # Format changes for email notification
        diff_lines = []
        for change in changes[:5]:
            diff_dict = {'Identifiers': change['identifiers']}
            diff_dict[f"{change['column']} (Old)"] = change['old_value']
            diff_dict[f"{change['column']} (New)"] = change['new_value']
            diff_lines.append(diff_dict)

        notify_updated_data(
            file_name,
            diff_lines,
            reason=f"Updated values in {total_changes} rows"
        )
        return True

    print(f"[{timestamp}] No new data to upload. Skipping GitHub update.")
    return False