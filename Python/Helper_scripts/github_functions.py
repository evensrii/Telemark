# github_functions.py

import requests
import os
import sys
from dotenv import load_dotenv
from datetime import datetime
import base64
import pandas as pd


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
    env_file_path = os.path.join(pythonpath, "Queries", "token.env")
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
