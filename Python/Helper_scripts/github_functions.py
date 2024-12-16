# github_functions.py

import requests
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
import base64
import pandas as pd
import numpy as np

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
    If column names differ between the new and old data, the new file replaces the old one.

    Parameters:
        input_df (pd.DataFrame): The DataFrame to compare and upload.
        file_name (str): The name of the file to be saved and compared.
        github_folder (str): The folder path in the GitHub repository.
        temp_folder (str): The local temporary folder for storing files.

    Returns:
        bool: True if new data is uploaded or detected, False otherwise.
    """
    # Save the DataFrame to a CSV in the Temp folder
    local_file_path = os.path.join(temp_folder, file_name)
    input_df.to_csv(local_file_path, index=False)

    # GitHub configuration
    github_file_path = f"{github_folder}/{file_name}"

    # Download the existing file from GitHub
    existing_data = download_github_file(github_file_path)

    # Check for header changes first using a small sample
    new_sample = pd.read_csv(local_file_path, nrows=1)
    original_existing = existing_data.columns.tolist()
    original_new = new_sample.columns.tolist()
    
    if set(original_existing) != set(original_new):
        print("\n=== Header Changes Detected ===")
        print("Old headers:", original_existing)
        print("New headers:", original_new)
        print("=" * 20 + "\n")
        upload_github_file(
            local_file_path,
            github_file_path,
            message=f"Updated {file_name} with new column headers"
        )
        return True

    # Initialize chunked reading
    chunk_size = 1000
    has_changes = False
    total_rows_new = sum(1 for _ in open(local_file_path)) - 1  # -1 for header
    total_rows_existing = len(existing_data)
    
    if total_rows_new != total_rows_existing:
        print(f"Dataset sizes differ. New: {total_rows_new}, Existing: {total_rows_existing}")
        has_changes = True
    else:
        # Read and compare files in chunks
        chunks_new = pd.read_csv(local_file_path, chunksize=chunk_size)
        chunks_existing = np.array_split(existing_data, len(existing_data) // chunk_size + 1)
        
        for chunk_idx, (chunk_new, chunk_existing) in enumerate(zip(chunks_new, chunks_existing)):
            # Standardize column names
            chunk_new = chunk_new.rename(columns=lambda x: x.strip().lower())
            chunk_existing = chunk_existing.rename(columns=lambda x: x.strip().lower())
            
            if not chunk_new.equals(chunk_existing):
                has_changes = True
                break
            
            print(f"Compared chunk {chunk_idx + 1} ({(chunk_idx + 1) * chunk_size} rows)", end='\r')

    if has_changes:
        print("\nNew data detected. Checking last 200 rows for specific changes...")
    else:
        print("\nNo differences found in the dataset.")
        return False

    # For the last 200 rows comparison, read only the necessary parts
    skiprows = max(0, total_rows_new - 200)
    last_200_new = pd.read_csv(local_file_path, skiprows=skiprows)
    last_200_existing = existing_data.tail(200)

    # Standardize column names for the last 200 rows
    last_200_new = last_200_new.rename(columns=lambda x: x.strip().lower())
    last_200_existing = last_200_existing.rename(columns=lambda x: x.strip().lower())

    # Identify numeric columns from the last 200 rows sample
    numeric_cols = last_200_new.select_dtypes(include=['float64', 'int64']).columns
    key_cols = [col for col in last_200_new.columns if col not in numeric_cols]

    print("\nDebug information:")
    print(f"Numeric columns detected: {numeric_cols}")
    print(f"Key columns detected: {key_cols}")

    # Merge the last 200 rows
    comparison = last_200_new.merge(
        last_200_existing,
        on=key_cols,
        how='outer',
        suffixes=('_new', '_old')
    )

    # Find rows where values have changed
    changed_rows = comparison[
        comparison.apply(lambda row: any(
            str(row[f"{col}_old"]) != str(row[f"{col}_new"])
            for col in numeric_cols
            if f"{col}_old" in row.index and f"{col}_new" in row.index
            and pd.notna(row[f"{col}_old"]) and pd.notna(row[f"{col}_new"])
        ), axis=1)
    ]

    # Print comparison details
    total_changes = len(changed_rows)
    
    if total_changes == 0:
        if has_changes:
            print("\nNo differences found in the last 200 rows, but the dataset has changed elsewhere.")
        else:
            print("\nNo differences found in the dataset.")
            return False

    if total_changes > 0:
        print("\n=== Data Comparison ===")
        print(f"File: {file_name}")
        if total_changes > 5:
            print(f"These are 5 of the {total_changes} differences found in the last 200 rows:\n")
        else:
            print(f"Found {total_changes} differences in the last 200 rows:\n")
        
        for idx, row in changed_rows.head(5).iterrows():
            # Print identifying information
            print("Row identifiers:")
            for col in key_cols:
                if pd.notna(row[col]) and str(row[col]) != 'nan':
                    print(f"  {col}: {row[col]}")
            
            # Print value changes
            print("Value changes:")
            for col in numeric_cols:
                old_col = f"{col}_old"
                new_col = f"{col}_new"
                if old_col in row.index and new_col in row.index:
                    if pd.notna(row[old_col]) and pd.notna(row[new_col]):
                        print(f"  {col}: {row[old_col]} -> {row[new_col]}")
            print()  # Empty line between rows

        print("=" * 20 + "\n")

    # Format changes for email notification
    diff_lines = changed_rows.head(5) if not changed_rows.empty else []

    if has_changes:
        upload_github_file(
            local_file_path, 
            github_file_path, 
            message=f"Updated {file_name}" + (f" with {total_changes} value changes in last 200 rows" if total_changes > 0 else "")
        )
        return True
    
    return False
