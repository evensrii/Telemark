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
        None
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
        new_df = pd.read_csv(local_file_path).rename(
            columns=lambda x: x.strip().lower()
        )

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
            return

        # Find common columns for comparison
        common_columns = [col for col in existing_df.columns if col in new_df.columns]
        existing_df = (
            existing_df[common_columns].astype(str).sort_values(by=common_columns)
        )
        new_df = new_df[common_columns].astype(str).sort_values(by=common_columns)

        # Compare the filtered DataFrames
        if existing_df.equals(new_df):
            print("No new data to upload. Skipping GitHub update.")
            return False
        else:
            print("New data detected. Uploading to GitHub.")

            # Compute differences
            diff = pd.concat([existing_df, new_df]).drop_duplicates(keep=False)
            diff_lines = diff.head(5).to_dict(orient="records")  # Show first 5 changes

            upload_github_file(
                local_file_path, github_file_path, message=f"Updated {file_name}"
            )
            # Notify about the updated data and differences
            notify_updated_data(file_name, diff_lines, reason="New data detected.")

            return True  # Eksisterende datasett er oppdatert
    else:
        # If the file does not exist on GitHub, upload the new file
        print("Uploading new file.")
        upload_github_file(
            local_file_path, github_file_path, message=f"Added {file_name}"
        )
        return True  # Nytt datasett er lastet opp
