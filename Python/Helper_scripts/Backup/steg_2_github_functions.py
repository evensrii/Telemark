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
    existing_df = existing_data.rename(columns=lambda x: x.strip().lower())
    new_df = pd.read_csv(local_file_path).rename(columns=lambda x: x.strip().lower())

    # Compare columns to detect mismatches
    if set(existing_df.columns) != set(new_df.columns):
        print(
            "Column names differ, probably caused by new year. Replacing file on GitHub with new data."
        )
        upload_github_file(
            local_file_path,
            github_file_path,
            message=f"Replaced {file_name} due to column name changes",
        )
        notify_updated_data(
            file_name, diff_lines=None, reason="Column names differ."
        )
        return True  # New data due to column name differences

    # Find common columns for comparison
    common_columns = [col for col in existing_df.columns if col in new_df.columns]
    existing_df = (
        existing_df[common_columns].astype(str).sort_values(by=common_columns)
    )
    new_df = new_df[common_columns].astype(str).sort_values(by=common_columns)

    # Compare the filtered DataFrames
    if existing_df.equals(new_df):
        print(f"[{timestamp}] No new data to upload. Skipping GitHub update.")
        return False  # No new data
    else:
        print(f"[{timestamp}] New data detected. Uploading to GitHub.")

        # Identify the key columns (all columns except numeric ones that might change)
        numeric_cols = new_df.select_dtypes(include=['float64', 'int64']).columns
        key_cols = [col for col in new_df.columns if col not in numeric_cols]

        # Merge old and new data to compare values
        comparison = existing_df.merge(
            new_df, 
            on=key_cols, 
            how='outer',
            suffixes=('_old', '_new')
        )

        # Find rows where values have changed
        changed_rows = comparison[
            comparison.apply(lambda row: any(
                row[f"{col}_old"] != row[f"{col}_new"]
                for col in numeric_cols
                if f"{col}_old" in row.index and f"{col}_new" in row.index
                and pd.notna(row[f"{col}_old"]) and pd.notna(row[f"{col}_new"])
            ), axis=1)
        ]

        # Print comparison details to log
        print(f"\n[{timestamp}] === Data Comparison ===")
        print(f"File: {file_name}")
        print("Changes detected in the following rows:\n")
        
        for idx, row in changed_rows.head(5).iterrows():
            # Print identifying information
            print("Row identifiers:")
            for col in key_cols:
                print(f"  {col}: {row[col]}")
            
            # Print value changes
            print("Value changes:")
            for col in numeric_cols:
                old_col = f"{col}_old"
                new_col = f"{col}_new"
                if old_col in row.index and new_col in row.index:
                    if pd.notna(row[old_col]) and pd.notna(row[new_col]):
                        print(f"  {col}: {row[old_col]} → {row[new_col]}")
            print()  # Empty line between rows

        # Show summary
        total_changes = len(changed_rows)
        print(f"Total rows with changes: {total_changes}")
        if total_changes > 5:
            print("(Showing first 5 changes only)")
        print("=" * 20 + "\n")

        # Format changes for email notification
        diff_lines = []
        for _, row in changed_rows.head(5).iterrows():
            change_dict = {col: row[col] for col in key_cols}
            for col in numeric_cols:
                old_col = f"{col}_old"
                new_col = f"{col}_new"
                if old_col in row.index and new_col in row.index:
                    if pd.notna(row[old_col]) and pd.notna(row[new_col]):
                        change_dict[f"{col} (Old)"] = row[old_col]
                        change_dict[f"{col} (New)"] = row[new_col]
            diff_lines.append(change_dict)

        upload_github_file(
            local_file_path, 
            github_file_path, 
            message=f"Updated {file_name} with {total_changes} value changes"
        )
        
        # Notify about the updated data and differences
        notify_updated_data(
            file_name, 
            diff_lines, 
            reason=f"Updated values in {total_changes} rows"
        )
        return True  # New data detected and uploaded