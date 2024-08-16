# github_functions.py

import requests
import os
from dotenv import load_dotenv
import datetime as dt

# Load GitHub token from .env file
dotenv_path = os.path.join(os.path.dirname(__file__), "token.env")
load_dotenv(dotenv_path)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

if GITHUB_TOKEN is None:
    raise ValueError("GitHub token is not found in 'token.env'")


def upload_file_to_github(source_file, destination_folder, github_repo, git_branch):
    """
    Upload a file to a specified GitHub repository branch.

    :param source_file: Path to the local file to be uploaded.
    :param destination_folder: The destination folder in the GitHub repository.
    :param github_repo: The GitHub repository in the format 'owner/repo'.
    :param git_branch: The branch in the repository where the file will be uploaded.
    """
    api_url = f"https://api.github.com/repos/{github_repo}/contents/{destination_folder}/{os.path.basename(source_file)}"

    with open(source_file, "rb") as file:
        content = file.read()

    # Encode file content to base64
    import base64

    content_base64 = base64.b64encode(content).decode("utf-8")

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Check if the file already exists
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        file_info = response.json()
        sha = file_info.get("sha")
    else:
        sha = None

    ## Timestamp

    # Current time for commit message
    timestamp = dt.datetime.now()

    # Commit message with time stamp
    message = f"Updated file at {timestamp}"

    data = {
        "message": message,
        "content": content_base64,
        "branch": git_branch,
    }

    if sha:
        # Update existing file
        data["sha"] = sha
        response = requests.put(api_url, headers=headers, json=data)
    else:
        # Create new file
        response = requests.put(api_url, headers=headers, json=data)

    if response.status_code in (200, 201):
        print(f"File uploaded successfully: {response.json()}")
    else:
        print(f"Failed to upload file: {response.json()}")


""" SHA (dvs. hvis endringer): Git assigns each commit a unique ID, called a SHA or hash, that identifies:

The specific changes
When the changes were made
Who created the changes """
