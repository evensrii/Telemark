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

    # Check if new data exists compared to GitHub
    if existing_data is not None:
        # Standardize column names for comparison
        existing_df = existing_data.rename(columns=lambda x: x.strip().lower())
        new_df = pd.read_csv(local_file_path).rename(columns=lambda x: x.strip().lower())

        # Identify numeric columns (only actual numeric columns, no conversion attempts)
        numeric_cols = new_df.select_dtypes(include=['float64', 'int64']).columns
        key_cols = [col for col in new_df.columns if col not in numeric_cols]

        print("\nDebug information:")
        print(f"Numeric columns detected: {numeric_cols}")
        print(f"Key columns detected: {key_cols}")
        print(f"Total rows in new data: {len(new_df)}")
        print(f"Total rows in existing data: {len(existing_df)}")

        # For large datasets, process in chunks
        chunk_size = 1000
        changed_rows = []
        total_changes = 0

        # Sort both dataframes by key columns for efficient comparison
        existing_df = existing_df.sort_values(by=key_cols)
        new_df = new_df.sort_values(by=key_cols)

        # Process in chunks
        for start_idx in range(0, len(new_df), chunk_size):
            end_idx = min(start_idx + chunk_size, len(new_df))
            chunk_new = new_df.iloc[start_idx:end_idx]
            
            # Find matching rows in existing data
            chunk_comparison = chunk_new.merge(
                existing_df,
                on=key_cols,
                how='left',
                suffixes=('_new', '_old')
            )

            # Find changes in this chunk
            chunk_changes = chunk_comparison[
                chunk_comparison.apply(lambda row: any(
                    str(row[f"{col}_old"]) != str(row[f"{col}_new"])
                    for col in numeric_cols
                    if f"{col}_old" in row.index and f"{col}_new" in row.index
                    and pd.notna(row[f"{col}_old"]) and pd.notna(row[f"{col}_new"])
                ), axis=1)
            ]

            # Store only the changed rows
            if not chunk_changes.empty:
                changed_rows.extend(chunk_changes.to_dict('records'))
                total_changes += len(chunk_changes)

            # Print progress
            print(f"Processed rows {start_idx} to {end_idx}, found {len(chunk_changes)} changes")

        # Print comparison details to log
        print("\n=== Data Comparison ===")
        print(f"File: {file_name}")
        print("Changes detected in the following rows:\n")
        
        # Show the first 5 changes
        for row in changed_rows[:5]:
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
                if old_col in row and new_col in row:
                    if pd.notna(row[old_col]) and pd.notna(row[new_col]):
                        print(f"  {col}: {row[old_col]} -> {row[new_col]}")
            print()  # Empty line between rows

        # Show summary
        print(f"Total rows with changes: {total_changes}")
        if total_changes > 5:
            print("(Showing first 5 changes only)")
        print("=" * 20 + "\n")

        # Format changes for email notification
        diff_lines = changed_rows[:5]

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
    else:
        # If the file does not exist on GitHub, upload the new file
        print("Uploading new file.")
        upload_github_file(
            local_file_path, github_file_path, message=f"Added {file_name}"
        )
        return True  # New file uploaded
