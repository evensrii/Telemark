# github_functions.py

import requests
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
import base64
import pandas as pd
from io import StringIO

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
        print(f"\nFile uploaded successfully: {github_file_path}")
    else:
        print(f"Failed to upload file: {response.json()}")


## Function to compare file to GitHub
def compare_to_github(input_df, file_name, github_folder, temp_folder):
    """
    Compares a DataFrame to an existing file on GitHub, and uploads if changes are detected.
    
    Args:
        input_df (pd.DataFrame): The DataFrame to compare and upload
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

    # Ensure all string columns use UTF-8 encoding
    for col in input_df.columns:
        if input_df[col].dtype == 'object':
            input_df[col] = input_df[col].astype(str).str.encode('utf-8').str.decode('utf-8')

    # Save the new data to temp folder
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
            
        # File exists, read it into DataFrame
        existing_data = existing_data
        
        # Ensure UTF-8 encoding for existing data
        for col in existing_data.columns:
            if existing_data[col].dtype == 'object':
                existing_data[col] = existing_data[col].astype(str).str.encode('utf-8').str.decode('utf-8')

        # Quick full comparison to determine if data is new/updated/same
        if existing_data.equals(input_df):
            log_message("No differences found in the dataset.")
            return False

        # If data is different, proceed with detailed comparison of last 200 rows
        log_message("Changes detected in the dataset. Analyzing differences...")

        # STEP 1: Check header changes
        existing_headers = [h.lower() for h in existing_data.columns]
        new_headers = [h.lower() for h in input_df.columns]

        # Check structural changes (ignoring case)
        if set(existing_headers) != set(new_headers):
            log_message("Header structure has changed.")
            upload_github_file(temp_file_path, f"{github_folder}/{file_name}", 
                             message=f"Updated {file_name} - header structure changed")
            return True

        # Check for year changes in headers
        for old_h, new_h in zip(existing_data.columns, input_df.columns):
            old_years = [int(s) for s in old_h.split() if s.isdigit()]
            new_years = [int(s) for s in new_h.split() if s.isdigit()]
            if old_years != new_years:
                log_message(f"Year in header changed: {old_h} -> {new_h}")
                upload_github_file(temp_file_path, f"{github_folder}/{file_name}",
                                 message=f"Updated {file_name} - year in headers changed")
                return True

        # STEP 2: Check row count
        if len(existing_data) != len(input_df):
            log_message(f"Row count changed from {len(existing_data)} to {len(input_df)}")
            upload_github_file(temp_file_path, f"{github_folder}/{file_name}",
                             message=f"Updated {file_name} - row count changed from {len(existing_data)} to {len(input_df)}")
            return True

        # STEP 3: Check value changes in last 200 rows
        last_200_existing = existing_data.tail(200).reset_index(drop=True)
        last_200_new = input_df.tail(200).reset_index(drop=True)

        # Identify numeric and key columns
        numeric_cols = []
        for col in last_200_new.columns:
            if col.lower() not in ['kommunenummer', 'kommunenr','Kommunenummer', 'Kommunenr']:  # Treat these as strings
                try:
                    pd.to_numeric(last_200_new[col])
                    numeric_cols.append(col)
                except (ValueError, TypeError):
                    pass

        differences_found = False
        diff_count = 0
        
        for idx in range(len(last_200_existing)):
            for col in last_200_existing.columns:
                old_val = str(last_200_existing.loc[idx, col])
                new_val = str(last_200_new.loc[idx, col])
                
                if old_val != new_val:
                    if diff_count < 5:  # Only show first 5 differences
                        log_message(f"Value changed in row {idx + len(existing_data) - 200}:")
                        log_message(f"  Column: {col}")
                        log_message(f"  Old value: {old_val}")
                        log_message(f"  New value: {new_val}")
                    differences_found = True
                    diff_count += 1
                    if diff_count == 5:
                        log_message("(Additional differences found...)")
                        break
            if diff_count == 5:
                break

        if differences_found:
            log_message("Uploading updated dataset to GitHub...")
            upload_github_file(temp_file_path, f"{github_folder}/{file_name}",
                             message=f"Updated {file_name} - value changes detected")
            return True
        else:
            log_message("No differences found in the last 200 rows, but the dataset has changed elsewhere.")
            upload_github_file(temp_file_path, f"{github_folder}/{file_name}",
                             message=f"Updated {file_name} - changes detected")
            return True

    finally:
        # Clean up temp file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            log_message(f"Deleted file from temp folder: {file_name}\n")