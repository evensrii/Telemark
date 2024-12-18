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
    Compares a DataFrame to an existing file on GitHub and uploads if changes are detected.
    Only previews differences in the last 200 rows for performance reasons.
    
    Args:
        input_df (pd.DataFrame): DataFrame to compare and upload
        file_name (str): Name of the file to save and compare
        github_folder (str): Folder path in GitHub repository
        temp_folder (str): Local temporary folder for storing files
    
    Returns:
        bool: True if new data uploaded or detected, False otherwise
    """
    def log_message(msg):
        """Add timestamp to log messages"""
        timestamp = datetime.now().strftime('[%d.%m.%Y %H:%M:%S,%f]')[:-4]
        print(f"{timestamp} {msg}")

    def standardize_dates_for_comparison(df):
        """Standardize dates for comparison only, preserving original format"""
        df = df.copy()
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]' or isinstance(df[col].dtype, pd.DatetimeTZDtype):
                df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
            elif 'år' in str(col).lower() or 'year' in str(col).lower():
                try:
                    # First check if it's already in year-only format
                    if df[col].astype(str).str.match(r'^\d{4}$').all():
                        continue
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
                except:
                    pass
        return df

    # Save the original data to temp folder (preserving original format)
    temp_file_path = os.path.join(temp_folder, file_name)
    input_df.to_csv(temp_file_path, index=False, encoding='utf-8')

    try:
        # Try to download existing file from GitHub
        existing_data = download_github_file(f"{github_folder}/{file_name}")
        
        if existing_data is None:
            # File doesn't exist on GitHub yet
            log_message("New dataset. Uploading to GitHub...")
            upload_github_file(temp_file_path, f"{github_folder}/{file_name}",
                             message=f"Added new dataset: {file_name}")
            return True

        # Create copies for comparison with standardized dates
        comparison_input = standardize_dates_for_comparison(input_df)
        comparison_existing = standardize_dates_for_comparison(existing_data)

        # STEP 1: Check header changes
        existing_headers = [str(h).strip().lower() for h in existing_data.columns]
        new_headers = [str(h).strip().lower() for h in input_df.columns]

        # Check structural changes (ignoring case)
        if set(existing_headers) != set(new_headers):
            log_message("Header structure has changed.")
            log_message(f"Old headers: {existing_data.columns.tolist()}")
            log_message(f"New headers: {input_df.columns.tolist()}")
            upload_github_file(temp_file_path, f"{github_folder}/{file_name}",
                             message=f"Updated {file_name} - header structure changed")
            return True

        # Check for year changes in headers
        for old_h, new_h in zip(existing_data.columns, input_df.columns):
            old_years = [int(s) for s in str(old_h).split() if s.isdigit()]
            new_years = [int(s) for s in str(new_h).split() if s.isdigit()]
            if old_years != new_years:
                log_message(f"Year in header changed: {old_h} -> {new_h}")
                upload_github_file(temp_file_path, f"{github_folder}/{file_name}",
                                 message=f"Updated {file_name} - year in headers changed")
                return True

        # STEP 2: Check row count
        if len(comparison_existing) != len(comparison_input):
            log_message(f"Row count changed from {len(comparison_existing)} to {len(comparison_input)}")
            upload_github_file(temp_file_path, f"{github_folder}/{file_name}",
                             message=f"Updated {file_name} - row count changed")
            return True

        # Quick full comparison to check if any changes exist
        if comparison_existing.equals(comparison_input):
            log_message("No differences found in the dataset.")
            return False

        # STEP 3: Check value changes in last 200 rows (or all rows if fewer than 200)
        log_message("Changes detected in the dataset. Analyzing differences...")
        
        # Get the minimum length between the two dataframes
        min_rows = min(len(comparison_existing), len(comparison_input))
        rows_to_check = min(200, min_rows)
        
        last_rows_existing = comparison_existing.tail(rows_to_check).reset_index(drop=True)
        last_rows_new = comparison_input.tail(rows_to_check).reset_index(drop=True)

        differences_found = False
        diff_count = 0
        
        for idx in range(len(last_rows_existing)):
            for col in last_rows_existing.columns:
                # Skip Kommunenummer columns
                if col.lower() in ['kommunenummer', 'kommunenr']:
                    continue
                    
                old_val = str(last_rows_existing.loc[idx, col])
                new_val = str(last_rows_new.loc[idx, col])
                
                if old_val != new_val:
                    if diff_count < 5:  # Only show first 5 differences
                        # Get original values for logging
                        orig_old_val = str(existing_data.iloc[idx + len(existing_data) - rows_to_check][col])
                        orig_new_val = str(input_df.iloc[idx + len(input_df) - rows_to_check][col])
                        log_message(f"Value changed in row {idx + len(existing_data) - rows_to_check}:")
                        log_message(f"  Column: {col}")
                        log_message(f"  Old value: {orig_old_val}")
                        log_message(f"  New value: {orig_new_val}")
                    differences_found = True
                    diff_count += 1
                    if diff_count == 5:
                        log_message("(Additional differences found...)")
                        break
            if diff_count == 5:
                break

        if differences_found:
            log_message("Uploading updated dataset to GitHub...")
        else:
            log_message("Changes detected in dataset, but not in the last 200 rows...")
            
        upload_github_file(temp_file_path, f"{github_folder}/{file_name}",
                         message=f"Updated {file_name} - value changes detected")
        return True

    except Exception as e:
        log_message(f"Error during comparison: {str(e)}")
        return False